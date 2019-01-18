
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.WindowsAzure.Storage.Table;
    using Microsoft.WindowsAzure.Storage.Auth;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    internal sealed class RestoreExecutor
    {
        /// <summary>
        /// CloudStorageAccount instance retrieved after successfully establishing a connection to the specified Blob Storage Account,
        /// into which created and updated documents will be backed up
        /// </summary>
        private CloudStorageAccount storageAccount;

        /// <summary>
        /// CloudBlobClient instance used to read backups from the specified Blob Storage Account
        /// </summary>
        private CloudBlobClient CloudBlobClient;

        /// <summary>
        /// Start Time from which to restore previously backed up changes
        /// </summary>
        private DateTime StartTimeForRestore;

        /// <summary>
        /// End time at which to stop the restore process
        /// </summary>
        private DateTime EndTimeForRestore;

        /// <summary>
        /// Instance of the DocumentClient, used to push successfully restored blob files
        /// as well as keeping track of hosts with leases on specific Azure Blob Containers
        /// </summary>
        private DocumentClient DocumentClient;

        /// <summary>
        /// Host name of this instance running the Restore job
        /// </summary>
        private string HostName;

        /// <summary>
        /// Max number of retries on rate limited writes to Cosmos DB
        /// </summary>
        private int MaxRetriesOnDocumentClientExceptions = 10;

        public RestoreExecutor(DocumentClient documentClient, string hostName)
        {
            this.DocumentClient = documentClient;
            this.HostName = hostName;
                
            // Retrieve the connection string for use with the application. The storage connection string is stored
            // in an environment variable on the machine running the application called storageconnectionstring.
            // If the environment variable is created after the application is launched in a console or with Visual
            // Studio, the shell or application needs to be closed and reloaded to take the environment variable into account.
            string storageConnectionString = ConfigurationManager.AppSettings["blobStorageConnectionString"];

            // Verify the Blob Storage Connection String
            if (CloudStorageAccount.TryParse(storageConnectionString, out this.storageAccount))
            {
                this.CloudBlobClient = storageAccount.CreateCloudBlobClient();
            }
            else
            {
                // Let the user know that they need to define the environment variable.
                throw new ArgumentException("The connection string for the Blob Storage Account is invalid. ");
            }

            this.ExtractStartAndEndTimeForRestore();
        }

        /// <summary>
        /// Extract the start and end time for restoring the backed up data. 
        /// If no start time is provided, the data is restored from the earliest backed up document.
        /// If no end time is provided, the data is restored to the most recently backed up document.
        /// </summary>
        private void ExtractStartAndEndTimeForRestore()
        {
            try
            {
                string startTimeForRestoreAsString = ConfigurationManager.AppSettings["startTimeForRestore"];

                if (!string.IsNullOrEmpty(startTimeForRestoreAsString))
                {
                    this.StartTimeForRestore = DateTime.ParseExact(startTimeForRestoreAsString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                }
            }
            catch (ArgumentNullException ex)
            {
                // If no specific start time is entered by the user, the restore will start from the beginning
                this.StartTimeForRestore = DateTime.MinValue;
            }
            catch (FormatException ex)
            {
                throw new ArgumentException("Invalid format for start time for restore. The correct format is: yyyy-MM-dd HH:mm:ss");
            }

            try
            {
                string endTimeForRestoreAsString = ConfigurationManager.AppSettings["endTimeForRestore"];

                this.EndTimeForRestore = DateTime.ParseExact(endTimeForRestoreAsString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }
            catch (ArgumentNullException ex)
            {
                // If no specific start time is entered by the user, the restore will start from the beginning
                this.EndTimeForRestore = DateTime.MaxValue;
            }
            catch (FormatException ex)
            {
                throw new ArgumentException("Invalid format for end time for restore. The correct format is: yyyy-MM-dd HH:mm:ss");
            }
        }

        /// <summary>
        /// Restores the previously backed up documents from the Blob Storage Account
        /// </summary>
        /// <returns></returns>
        public async Task ExecuteRestore()
        {
            // Fetch the list of blob containers to restore data from
            List<string> containerNames = this.GetListOfContainersInStorageAccount();

            // Determine the first Blob Storage container to acquire a lease on and restore to Cosmos DB
            string blobContainerToRestore = await GetNextContainerToRestore(containerNames);

            while (!string.IsNullOrEmpty(blobContainerToRestore))
            {
                // Get the list of blob files within the container(s) that satisfy the time windows for the restore
                List<string> listOfBlobsToRestore = GetListOfBlobsToRestore(blobContainerToRestore);

                // Read the Blob files, de-compress the data, and restore the documents to the new Cosmos DB collection
                await this.RestoreDataFromBlobFile(blobContainerToRestore, listOfBlobsToRestore);

                // Determine the next Blob Storage container to acquire a lease on and restore to Cosmos DB
                blobContainerToRestore = await GetNextContainerToRestore(containerNames);
            }
        }

        /// <summary>
        /// Retrieves the number of documents in the RestoreHelperCollection.
        /// This collection stores 1 lease document for every container in the Blob Storage Account.
        /// If the count of documents in this collection equals the number of containers in the Blob Storage account, there are
        /// no additional containers to restore to the specified Cosmos DB account/collection.
        /// </summary>
        /// <returns></returns>
        private async Task<long> GetDocumentCountInBlobStorageLeaseCollection()
        {
            string restoreHelperDatabaseName = ConfigurationManager.AppSettings["RestoreHelperDatabaseName"];
            string restoreHelperCollectionName = ConfigurationManager.AppSettings["RestoreHelperCollectionName"];
            string documentCollectionLink = UriFactory.CreateDocumentCollectionUri(restoreHelperDatabaseName, restoreHelperCollectionName).ToString();

            ResourceResponse<DocumentCollection> resourceResponse =
                await this.DocumentClient.ReadDocumentCollectionAsync(documentCollectionLink, new RequestOptions { PopulatePartitionKeyRangeStatistics = true });

            long documentCount = 0;
            foreach (PartitionKeyRangeStatistics eachPartitionsStats in resourceResponse.Resource.PartitionKeyRangeStatistics)
            {
                documentCount += eachPartitionsStats.DocumentCount;
            }

            return documentCount;
        }

        /// <summary>
        /// Restore each JObject in the input JArray into the specified Cosmos DB collection
        /// </summary>
        /// <param name="jArray"></param>
        /// <returns></returns>
        private async Task WriteJArrayToCosmosDB(JArray jArray)
        {
            string restoreHelperDatabaseName = ConfigurationManager.AppSettings["RestoreHelperDatabaseName"];
            string restoreHelperCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreHelperDatabaseName, restoreHelperCollectionName);

            int countOfDocumentsWritten = 0;
            int numRetries = 0;

            foreach (JObject eachJObject in jArray)
            {
                if (VerifyTimeRangeForDocumentRestore(eachJObject))
                {
                    try
                    {
                        await this.DocumentClient.UpsertDocumentAsync(
                            documentsFeedLink,
                            eachJObject,
                            null,
                            true);

                        countOfDocumentsWritten++;
                    }
                    catch (DocumentClientException ex)
                    {
                        // Keep retrying when rate limited
                        if ((int)ex.StatusCode == 429)
                        {
                            Console.WriteLine("Received rate limiting exception when attempting to restore document. Retrying");

                            // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                            int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                            bool success = false;
                            while (!success && numRetries <= this.MaxRetriesOnDocumentClientExceptions)
                            {
                                // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                                Thread.Sleep(sleepTime);

                                try
                                {
                                    await this.DocumentClient.UpsertDocumentAsync(documentsFeedLink, eachJObject, null, true);
                                    success = true;
                                    countOfDocumentsWritten++;
                                }
                                catch (DocumentClientException e)
                                {
                                    sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                    numRetries++;
                                }
                                catch (Exception exception)
                                {
                                    Console.WriteLine("Caught Exception when retrying. Exception was: {0}. Will continue to retry.", exception.Message);
                                    numRetries++;
                                }
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("Caught exception when attempting to upsert document. Original exception message was: {0}", e.Message);
                    }
                }
                else
                {
                    Console.WriteLine("Document is not in restore time window. Skipping ...");
                }

                Console.WriteLine("Completed restoring {0}", countOfDocumentsWritten);
            }
        }

        /// <summary>
        /// Determines if the previously backed up document, is in the time window specified by the user for a restore
        /// </summary>
        /// <param name="jObject">JSON document previously backed up to Blob Store using Change Feed Processor</param>
        /// <returns></returns>
        private bool VerifyTimeRangeForDocumentRestore(JObject jObject)
        {
            bool isDocumentInRestoreWindow = false;

            // Convert the _ts property of the document to restore, to a timestamp
            DateTime timestampInDocument = new DateTime(1970, 1, 1).AddSeconds(long.Parse(jObject["_ts"].ToString()));
            if(DateTime.Compare(timestampInDocument, this.StartTimeForRestore) >= 0 && DateTime.Compare(timestampInDocument, this.EndTimeForRestore) <= 0)
            {
                isDocumentInRestoreWindow = true;
            }

            return isDocumentInRestoreWindow;
        }

        /// <summary>
        /// Reads from as many containers as specified, pulls down all blobs within the containers
        /// and restores the backed up documents into a new Cosmos DB collection
        /// </summary>
        /// <param name="containerName">Randomly determined containers to pull down from the Azure Blob Storage account</param>
        /// <param name="blobName"></param>
        private async Task RestoreDataFromBlobFile(string containerName, List<string> blobsPerContainerToRestore)
        {
            Console.WriteLine("Restoring required blobs from container: {0}", containerName);

            CloudBlobContainer blobContainer = this.CloudBlobClient.GetContainerReference(containerName);
            
            foreach (string blobName in blobsPerContainerToRestore)
            {
                // Verify that the document hasn't already been written on a previously unsuccessful restore job
                bool isBlobPreviouslyRestored = await VerifyPreviouslyRestoredBlob(containerName, blobName);

                if(!isBlobPreviouslyRestored)
                {
                    CloudBlockBlob blockBlob = blobContainer.GetBlockBlobReference(blobName);
                    blockBlob.FetchAttributes();

                    byte[] byteArray = new byte[blockBlob.Properties.Length];
                    blockBlob.DownloadToByteArray(byteArray, 0);

                    StringBuilder sB = new StringBuilder(byteArray.Length);
                    foreach (byte item in Decompress(byteArray))
                    {
                        sB.Append((char)item);
                    }

                    JArray jArray = JsonConvert.DeserializeObject<JArray>(sB.ToString());
                    await WriteJArrayToCosmosDB(jArray);

                    // If the Blob was successfully restored, update the success/failure tracking collection with the name of the restored Blob
                    await UpdateSuccessfullyRestoreBlob(containerName, blobName);
                }
            }
        }

        /// <summary>
        /// Verifies if the blob file containing previously backed up documents using ChangeFeedProcessor has already been restored during a previous run
        /// </summary>
        /// <param name="containerName">Blob Storage container name of the Blob that was successfully restored</param>
        /// <param name="blobName">Blob file that was successfully restored</param>
        /// <returns></returns>
        private async Task<bool> VerifyPreviouslyRestoredBlob(string containerName, string blobName)
        {
            string restoreSuccessFailureDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessFailureDatabaseName"];
            string restoreSuccessFailureCollectionName = ConfigurationManager.AppSettings["RestoreSuccessFailureCollectionName"];
            Uri documentsLink = UriFactory.CreateDocumentUri(restoreSuccessFailureDatabaseName, restoreSuccessFailureCollectionName, string.Concat(containerName, "-", blobName));

            int numRetries = 0;

            bool documentAlreadyRestored = false;
            try
            {
                await this.DocumentClient.ReadDocumentAsync(
                    documentsLink, 
                    new RequestOptions { PartitionKey = new PartitionKey(string.Concat(containerName, "-", blobName)) });
            
                Console.WriteLine("Blob: {0} from container: {1} has already been restored. SKIPPING...", containerName, blobName);

                documentAlreadyRestored = true;
            }
            catch (DocumentClientException ex)
            {
                if ((int)ex.StatusCode == 404)
                {
                    // The document has already been restored, simply return a status of false
                    documentAlreadyRestored = false;
                    Console.WriteLine("Blob: {0} from container: {1} has NOT been restored. RESTORING...", containerName, blobName);
                }
                else if ((int)ex.StatusCode == 429)
                {
                    Console.WriteLine("Received rate limiting exception when attempting to read document: {0}. Retrying", documentsLink);

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    // Custom retry logic to keep retrying when the document read is rate limited
                    bool success = false;
                    while (!success && numRetries <= this.MaxRetriesOnDocumentClientExceptions)
                    {
                        Console.WriteLine("Received rate limiting exception when attempting to read document: {0}. Retrying", documentsLink);

                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            await this.DocumentClient.ReadDocumentAsync(documentsLink);

                            // The document has already been restored, simply return a status of true for this blob to be skipped during restore
                            Console.WriteLine("Blob: {0} from container: {1} has already been restored. SKIPPING...", containerName, blobName);
                            success = true;
                            documentAlreadyRestored = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 404)
                            {
                                success = true;

                                // The document has not been restored, simply return a status of false for this blob to be picked up for restore
                                Console.WriteLine("Blob: {0} from container: {1} has NOT been restored. RESTORING...", containerName, blobName);
                                documentAlreadyRestored = false;
                            }
                        }
                        catch (Exception exception)
                        {
                            Console.WriteLine("Caught Exception when retrying. Exception was: {0}. Will continue to retry.", exception.Message);
                            numRetries++;
                        }
                    }
                }
            }

            return documentAlreadyRestored;
        }

        /// <summary>
        /// Update the collection tracking successfully restored blobs 
        /// by creating a document containing the names of the successfully restored blob file and its container
        /// </summary>
        /// <param name="containerName">Blob Storage container name of the Blob that was successfully restored</param>
        /// <param name="blobName">Blob file that was successfully restored</param>
        /// <returns></returns>
        private async Task UpdateSuccessfullyRestoreBlob(string containerName, string blobName)
        {
            string restoreSuccessFailureDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessFailureDatabaseName"];
            string restoreSuccessFailureCollectionName = ConfigurationManager.AppSettings["RestoreSuccessFailureCollectionName"];
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreSuccessFailureDatabaseName, restoreSuccessFailureCollectionName);

            BlobRestoreSuccessDocument blobRestoreSuccessDocument = new BlobRestoreSuccessDocument();
            blobRestoreSuccessDocument.ContainerName = containerName;
            blobRestoreSuccessDocument.BlobName = blobName;
            blobRestoreSuccessDocument.Status = "success";
            blobRestoreSuccessDocument.Id = string.Concat(containerName, "-", blobName);

            int numRetries = 0;

            try
            {
                await this.DocumentClient.UpsertDocumentAsync(documentsFeedLink, blobRestoreSuccessDocument, null, true);

                Console.WriteLine("Successfully update RestoreSucessFailureCollection with the successfully restored blob: {0} in container: {1}", blobName, containerName);
            }
            catch (DocumentClientException ex)
            {
                // Keep retrying when rate limited
                if ((int)ex.StatusCode == 429)
                {
                    Console.WriteLine("Received rate limiting exception when attempting to update successfully restored blob: {0}", blobName);

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= this.MaxRetriesOnDocumentClientExceptions)
                    {
                        Console.WriteLine("Waiting for twice the recommended time when rate limited");

                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            await this.DocumentClient.UpsertDocumentAsync(documentsFeedLink, blobRestoreSuccessDocument, null, true);

                            Console.WriteLine(
                                "Successfully update RestoreSucessFailureCollection with the successfully restored blob: {0} in container: {1}", 
                                blobName, 
                                containerName);

                            success = true;                            
                        }
                        catch (DocumentClientException e)
                        {
                            Console.WriteLine("Still received an exception. Original  exception was: {0}", e.Message);
                            sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            Console.WriteLine("Caught Exception when retrying. Exception was: {0}. Will continue to retry.", exception.Message);
                            numRetries++;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Caught Exception when retrying. Exception was: {0}. Will continue to retry.", ex.Message);
            }
        }

        /// <summary>
        /// Retrieves the list of blobs from the Azure Storage container
        /// to fetch data from and restore to the specified Cosmos DB collection.
        /// The blob files contain the min and max timestamps of all documents backed up
        /// within them. This is used to determine which blobs contain the documents to be restored
        /// (based on the min and max timestamps specified by the user for restoring the backed up data).
        /// </summary>
        /// <param name="containerName">Azure Storage Container with blobs containing compressed data backups</param>
        /// <returns></returns>
        private List<string> GetListOfBlobsToRestore(string containerName)
        {
            Console.WriteLine("Fetching list of blobs in container: {0}", containerName);

            List<string> blobsToRestore = new List<string>();
            CloudBlobContainer blobContainer = this.CloudBlobClient.GetContainerReference(containerName);

            foreach (IListBlobItem blobItem in blobContainer.ListBlobs())
            {
                string blobName = ((CloudBlockBlob)blobItem).Name;
                string[] blobNameComponents = blobName.Split('-');

                // Extract the min timestamp across all documents in the Blob file
                string startTimeForDocumentsInFile = blobNameComponents[0];
                DateTime startTimeInBlobFile = DateTime.ParseExact(startTimeForDocumentsInFile, "yyyyMMddTHH:mm:ss", CultureInfo.InvariantCulture);

                // Extract the max timestamp across all documents in the Blob file
                string endTimeForDocumentsInFile = blobNameComponents[1];
                DateTime endTimeInBlobFile = DateTime.ParseExact(endTimeForDocumentsInFile, "yyyyMMddTHH:mm:ss", CultureInfo.InvariantCulture);

                // Determine the Blob Files to use for the Restore, as specified by the Start and End times for the Restore operation
                if (DateTime.Compare(StartTimeForRestore, startTimeInBlobFile) >= 0 && DateTime.Compare(StartTimeForRestore, endTimeInBlobFile) <= 0)
                {
                    blobsToRestore.Add(blobName);
                    Console.WriteLine("Found in time range: Container: {0}, Blob: {1}", containerName, blobName);
                }
                else if (DateTime.Compare(StartTimeForRestore, startTimeInBlobFile) <= 0 && DateTime.Compare(EndTimeForRestore, startTimeInBlobFile) >= 0)
                {
                    blobsToRestore.Add(blobName);
                    Console.WriteLine("Found in time range: Container: {0}, Blob: {1}", containerName, blobName);
                }
            }

            return blobsToRestore;
        }

        /// <summary>
        /// Retrieves the list of blob containers in the storage account to restore into the specified Cosmos DB account/collection
        /// </summary>
        /// <returns></returns>
        private List<string> GetListOfContainersInStorageAccount()
        {
            Console.WriteLine("Fetching list of container names");

            List<string> containerNames = new List<string>();
            foreach (CloudBlobContainer eachContainer in this.CloudBlobClient.ListContainers())
            {
                if(eachContainer.Name.StartsWith("partition"))
                {
                    Console.WriteLine("Found container: {0}", eachContainer.Name);
                    containerNames.Add(eachContainer.Name);
                }
            }

            return containerNames;
        }

        /// <summary>
        /// Shuffle the list of container names retrieved from Blob Storage.
        /// This facilitates easier lease management on blob containers containing blobs to restore
        /// </summary>
        /// <typeparam name="E"></typeparam>
        /// <param name="inputList"></param>
        /// <returns></returns>
        private List<E> ShuffleList<E>(List<E> inputList)
        {
            List<E> randomList = new List<E>();

            Random r = new Random();
            int randomIndex = 0;
            while (inputList.Count > 0)
            {
                randomIndex = r.Next(0, inputList.Count); //Choose a random object in the list
                randomList.Add(inputList[randomIndex]); //add it to the new, random list
                inputList.RemoveAt(randomIndex); //remove to avoid duplicates
            }

            return randomList; //return the new random list
        }

        /// <summary>
        /// Determine the list of Azure Blob Storage containers from which to restore data into the new Cosmos DB collection
        /// </summary>
        /// <returns></returns>
        private async Task<string> GetNextContainerToRestore(List<string> containerNames)
        {
            List<string> shuffledContainerNames = new List<string>();
            shuffledContainerNames.AddRange(containerNames);

            shuffledContainerNames = this.ShuffleList(shuffledContainerNames);

            string containerName = "";
            bool success = false;
            int numRetries = 0;

            // Number of Blob Storage containers containing documents to restore
            long numberOfContainersToRestore = shuffledContainerNames.Count;

            // Number of containers in the Blob Storage account with leases acquired
            long numberOfDocumentsInBlobStorageLeaseCollection = await this.GetDocumentCountInBlobStorageLeaseCollection();

            do
            {
                // If leases have been acquired for all containers in the Blob Storage account, there are no more containers to process for restore
                if (numberOfDocumentsInBlobStorageLeaseCollection < numberOfContainersToRestore)
                {
                    string restoreHelperDatabaseName = ConfigurationManager.AppSettings["RestoreHelperDatabaseName"];
                    string restoreHelperCollectionName = ConfigurationManager.AppSettings["RestoreHelperCollectionName"];
                    Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreHelperDatabaseName, restoreHelperCollectionName);

                    foreach (string eachContainerToRestore in shuffledContainerNames)
                    {
                        if (!success)
                        {
                            // Verify the container has not already been picked up by another AppService instance
                            // by attempting to create a document in the HelperCollection
                            RestoreContainerDocument restoreContainerDocument = new RestoreContainerDocument();
                            restoreContainerDocument.RestoreHostName = this.HostName;
                            restoreContainerDocument.Id = eachContainerToRestore;
                            restoreContainerDocument.ContainerName = eachContainerToRestore;
                            try
                            {
                                Console.WriteLine("About to create document: {0}", JsonConvert.SerializeObject(restoreContainerDocument));
                                await this.DocumentClient.CreateDocumentAsync(documentsFeedLink, restoreContainerDocument, null, true);
                                success = true;
                                containerName = eachContainerToRestore;

                                Console.WriteLine("Successfully tracking container: {0}", containerName);
                            }
                            catch (DocumentClientException ex)
                            {
                                if ((int)ex.StatusCode == 409)
                                {
                                    Console.WriteLine("Conflict when attempting to pick up a lease on a container. Retrying a lease on a different container");
                                }
                                else
                                {
                                    Console.WriteLine("Other exception thrown. Status code == {0}", (int)ex.StatusCode);
                                }
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception thrown when attempting to pick up a lease on a container. Original exception was: {0}", ex.Message);
                            }
                        }
                    }

                    // Refresh count of leases which have been picked up for the Blob Storage containers to restore
                    numberOfDocumentsInBlobStorageLeaseCollection = await this.GetDocumentCountInBlobStorageLeaseCollection();

                    numRetries++;
                }
                else
                {
                    success = true;
                }

            } while (!success && numRetries <= this.MaxRetriesOnDocumentClientExceptions);

            Console.WriteLine("Next container to restore is {0}", containerName);

            return containerName;
        }

        /// <summary>
        /// Decompress the byte array retrieved from Blob Storage into the original array of documents retrieved from ChangeFeed
        /// </summary>
        /// <param name="gzip"></param>
        /// <returns></returns>
        private static byte[] Decompress(byte[] gzip)
        {
            using (GZipStream stream = new GZipStream(new MemoryStream(gzip),
                CompressionMode.Decompress))
            {
                const int size = 4096;
                byte[] buffer = new byte[size];
                using (MemoryStream memory = new MemoryStream())
                {
                    int count = 0;
                    do
                    {
                        count = stream.Read(buffer, 0, size);
                        if (count > 0)
                        {
                            memory.Write(buffer, 0, count);
                        }
                    }
                    while (count > 0);
                    return memory.ToArray();
                }
            }
        }
    }
}
