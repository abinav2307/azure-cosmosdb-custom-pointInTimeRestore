
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Linq;

    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.CosmosDB.PITRWithRestore.CosmosDB;
    using Microsoft.CosmosDB.PITRWithRestore.BlobStorage;
    using Microsoft.CosmosDB.PITRWithRestore.Backup;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Text;
    using System.Threading.Tasks;

    public class RestoreExecutor
    {
        /// <summary>
        /// CloudStorageAccount instance retrieved after successfully establishing a connection to the specified Blob Storage Account,
        /// into which created and updated documents will be backed up
        /// </summary>
        private CloudStorageAccount StorageAccount;

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
            if (CloudStorageAccount.TryParse(storageConnectionString, out this.StorageAccount))
            {
                this.CloudBlobClient = this.StorageAccount.CreateCloudBlobClient();
            }
            else
            {
                // Let the user know that they need to define the environment variable.
                throw new ArgumentException("The connection string for the Blob Storage Account is invalid. ");
            }

            this.CreateCollectionsForRestore();

            this.ExtractStartAndEndTimeForRestore();
        }

        /// <summary>
        /// Create the following collections if they don't already exist:
        /// 1. RestoreCollection        - Cosmos DB collection into which backup blobs will be restored
        /// 2. RestoreHelperCollection  - Cosmos DB collection keeping track of blob containers being restored by host machine(s)
        /// 3. RestoreSuccessCollection - Cosmos DB collection keeping track of successfully restored blob files. This collection
        ///                               will be read from if a restore host machine dies and another machine picks up restoring
        ///                               backups from where the dead machine stopped
        /// 4. RestoreFailureCollection - Cosmos DB collection containing blobs which could not successfully be restored 
        ///                               to the RestoreCollection in (1)
        /// </summary>
        private void CreateCollectionsForRestore()
        {
            // 1. Create the RestoreHelper collection
            string restoreHelperDatabaseName = ConfigurationManager.AppSettings["RestoreHelperDatabaseName"];
            string restoreHelperCollectionName = ConfigurationManager.AppSettings["RestoreHelperCollectionName"];
            int restoreHelperCollectionThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreHelperCollectionThroughput"]);
            string restoreHelperCollectionPartitionKey = ConfigurationManager.AppSettings["RestoreHelperCollectionPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                restoreHelperDatabaseName,
                restoreHelperCollectionName,
                restoreHelperCollectionThroughput,
                restoreHelperCollectionPartitionKey).Wait();

            // 2. Create the Restore collection, which will contain the restored documents
            string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            string restoreCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];
            int restoreCollectionThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreCollectionThroughput"]);
            string restoreCollectionPartitionKey = ConfigurationManager.AppSettings["RestoreCollectionPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                restoreDatabaseName,
                restoreCollectionName,
                restoreCollectionThroughput,
                restoreCollectionPartitionKey).Wait();

            // 3. Create the Restore Success collection, containing successfully restored blob filenames
            string restoreSuccessDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessDatabaseName"];
            string restoreSuccessCollectionName = ConfigurationManager.AppSettings["RestoreSuccessCollectionName"];
            int restoreSuccessCollectionThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreSuccessCollectionThroughput"]);
            string restoreSuccessCollectionPartitionKey = ConfigurationManager.AppSettings["RestoreSuccessCollectionPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                restoreSuccessDatabaseName,
                restoreSuccessCollectionName,
                restoreSuccessCollectionThroughput,
                restoreSuccessCollectionPartitionKey).Wait();

            // 4. Create the Restore Failure collection, containing blob filenames that were NOT successfully restored
            string restoreFailureDatabaseName = ConfigurationManager.AppSettings["RestoreFailureDatabaseName"];
            string restoreFailureCollectionName = ConfigurationManager.AppSettings["RestoreFailureCollectionName"];
            int restoreFailureCollectionThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreFailureCollectionThroughput"]);
            string restoreFailureCollectionPartitionKey = ConfigurationManager.AppSettings["RestoreFailureCollectionPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                restoreFailureDatabaseName,
                restoreFailureCollectionName,
                restoreFailureCollectionThroughput,
                restoreFailureCollectionPartitionKey).Wait();
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
            List<string> containerNames = BlobStorageHelper.GetListOfContainersInStorageAccount(this.CloudBlobClient);
            
            //Determine the first Blob Storage container to acquire a lease on and restore to Cosmos DB
            string blobContainerToRestore = await GetNextContainerToRestore(containerNames);

            while (!string.IsNullOrEmpty(blobContainerToRestore))
            {
                //First, go through the list of failed backups for this container and attempt to back them up again
                await this.BackupFailedBackups(blobContainerToRestore);

                // Get the list of blob files within the container(s) that satisfy the time windows for the restore
                List<string> listOfBlobsToRestore = GetListOfBlobsToRestore(blobContainerToRestore);

                // Read the Blob files, de-compress the data, and restore the documents to the new Cosmos DB collection
                await this.RestoreDataFromBlobFile(blobContainerToRestore, listOfBlobsToRestore);

                //Determine the next Blob Storage container to acquire a lease on and restore to Cosmos DB
                blobContainerToRestore = await GetNextContainerToRestore(containerNames);
            }

            await this.TestRebackedUpBackups();

            Console.WriteLine("Completed executing restore on host: {0}", this.HostName);
        }

        private async Task TestRebackedUpBackups()
        {
            string containerName = "backup-test";
            CloudBlobContainer blobContainer = this.CloudBlobClient.GetContainerReference(containerName);

            List<string> blobsPerContainerToRestore = new List<string>();
            foreach (IListBlobItem eachBlob in blobContainer.ListBlobs())
            {
                blobsPerContainerToRestore.Add(((CloudBlockBlob)eachBlob).Name);
            }

            foreach (string blobName in blobsPerContainerToRestore)
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
                List<JObject> documentsFailedToRestore = await WriteJArrayToCosmosDB(jArray);
            }
        }

        /// <summary>
        /// Once again attempt to backup failed backups for the container being restored
        /// </summary>
        /// <param name="containerToRestore">Name of the Blob Storage container containiner the backups to be restored</param>
        /// <returns></returns>
        private async Task BackupFailedBackups(string containerToRestore)
        {
            CloudBlobContainer cloudBlobContainer = this.CloudBlobClient.GetContainerReference(containerToRestore);
            cloudBlobContainer.CreateIfNotExists();

            string backupFailureDatabaseName = ConfigurationManager.AppSettings["BackupFailureDatabaseName"];
            string backupFailureCollectionName = ConfigurationManager.AppSettings["BackupFailureCollectionName"];

            string queryToExecute = string.Format("select * from c where c.containerName = '{0}'", containerToRestore);
            string collectionLink =
                UriFactory.CreateDocumentCollectionUri(backupFailureDatabaseName, backupFailureCollectionName).ToString();
            try
            {
                var query = this.DocumentClient.CreateDocumentQuery<BackupFailureDocument>(collectionLink,
                queryToExecute,
                new FeedOptions()
                {
                    MaxDegreeOfParallelism = -1,
                    MaxItemCount = -1,
                    EnableCrossPartitionQuery = true
                }).AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    var result = await query.ExecuteNextAsync();
                    foreach (BackupFailureDocument eachFailedBackupDocument in result)
                    {
                        if(eachFailedBackupDocument.ExceptionType != null)
                        {
                            // Extract the name of the blob as it should appear in the Blob Storage Account
                            string fileName = eachFailedBackupDocument.Id;

                            // Extract the Gzip compressed backup
                            string compressedBackupAsString = eachFailedBackupDocument.CompressedByteArray;

                            // Convert the string representation of the byte array back into a byte array
                            byte[] compressedByteArray = new byte[compressedBackupAsString.Length];
                            int indexBA = 0;
                            foreach (char item in compressedBackupAsString.ToCharArray())
                            {
                                compressedByteArray[indexBA++] = (byte)item;
                            }

                            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);

                            try
                            {
                                BlobStorageHelper.WriteToBlobStorage(
                                    blockBlob,
                                    compressedByteArray,
                                    this.MaxRetriesOnDocumentClientExceptions);

                                // Upon successfully backing up the previously failed backup,
                                // delete the document from the 'BackupFailureCollection'
                                await CosmosDBHelper.DeleteDocmentAsync(
                                    this.DocumentClient,
                                    backupFailureDatabaseName,
                                    backupFailureCollectionName,
                                    fileName,
                                    fileName,
                                    this.MaxRetriesOnDocumentClientExceptions);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine("Exception while attempting to backup previously failed backups: " + ex.Message);
                                Console.WriteLine("Stack trace is: {0}", ex.StackTrace);
                            }
                        }                        
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception message while querying for previously failed backups: " + ex.Message);
                Console.WriteLine("Stack trace is: {0}", ex.StackTrace);
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
            long documentCount = await CosmosDBHelper.FetchDocumentCountInCollection(this.DocumentClient, restoreHelperDatabaseName, restoreHelperCollectionName);

            return documentCount;
        }

        /// <summary>
        /// Restore each JObject in the input JArray into the specified Cosmos DB collection
        /// and store the documents that were not successfully restored into a collection tracking restore failures
        /// </summary>
        /// <param name="jArray"></param>
        /// <returns></returns>
        private async Task<List<JObject>> WriteJArrayToCosmosDB(JArray jArray)
        {
            string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            string restoreCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreDatabaseName, restoreCollectionName);

            int countOfDocumentsWritten = 0;

            List<JObject> documentsFailedToRestore = new List<JObject>();
            foreach (JObject eachJObject in jArray)
            {
                if (VerifyTimeRangeForDocumentRestore(eachJObject))
                {
                    try
                    {
                        ResourceResponse<Document> upsertedDocument = await CosmosDBHelper.UpsertDocumentAsync(
                            this.DocumentClient, 
                            restoreDatabaseName,
                            restoreCollectionName, 
                            eachJObject, 
                            this.MaxRetriesOnDocumentClientExceptions);

                        if(upsertedDocument != null)
                        {
                            countOfDocumentsWritten++;
                        }
                        else
                        {
                            documentsFailedToRestore.Add(eachJObject);
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

            return documentsFailedToRestore;
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
        /// <param name="blobsPerContainerToRestore">List of blobs containing Cosmos DB backups to be restored</param>
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
                    List<JObject> documentsFailedToRestore = await WriteJArrayToCosmosDB(jArray);

                    // If the Blob was successfully restored, update the success/failure tracking collection with the name of the restored Blob
                    if(documentsFailedToRestore.Count == 0)
                    {
                        await this.UpdateSuccessfullyRestoreBlob(containerName, blobName);
                    }
                    else
                    {
                        // Update documents that were not successfully restored to the specified Cosmos DB collection
                        await this.UpdateFailedRestoreBlob(containerName, blobName);
                    }
                    
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
            string restoreSuccessDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessDatabaseName"];
            string restoreSuccessCollectionName = ConfigurationManager.AppSettings["RestoreSuccessCollectionName"];
            
            bool documentAlreadyRestored = false;
            try
            {
                ResourceResponse<Document> restoredBlobDocument = await CosmosDBHelper.ReadDocmentAsync(
                    this.DocumentClient,
                    restoreSuccessDatabaseName, 
                    restoreSuccessCollectionName, 
                    string.Concat(containerName, "-", blobName), 
                    string.Concat(containerName, "-", blobName), 
                    this.MaxRetriesOnDocumentClientExceptions);

                if(restoredBlobDocument != null)
                {
                    documentAlreadyRestored = true;
                    Console.WriteLine("Blob: {0} from container: {1} has already been restored. SKIPPING...", containerName, blobName);
                }
                else
                {
                    documentAlreadyRestored = false;
                    Console.WriteLine("Blob: {0} from container: {1} has NOT been restored. RESTORING...", containerName, blobName);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception encountered when determining if Blob: {0} from container: {1} has already been restored. Will proceed to restore...", containerName, blobName);
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
            string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessDatabaseName"];
            string restoreSuccessCollectionName = ConfigurationManager.AppSettings["RestoreSuccessCollectionName"];
            
            BlobRestoreStatusDocument blobRestoreSuccessDocument = new BlobRestoreStatusDocument();
            blobRestoreSuccessDocument.ContainerName = containerName;
            blobRestoreSuccessDocument.BlobName = blobName;
            blobRestoreSuccessDocument.Status = "success";
            blobRestoreSuccessDocument.Id = string.Concat(containerName, "-", blobName);

            try
            {
                await CosmosDBHelper.UpsertDocumentAsync(
                    this.DocumentClient, 
                    restoreDatabaseName, 
                    restoreSuccessCollectionName, 
                    blobRestoreSuccessDocument, 
                    this.MaxRetriesOnDocumentClientExceptions);
                
                Console.WriteLine("Successfully updated RestoreSucessCollection with the successfully restored blob: {0} in container: {1}", blobName, containerName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Caught Exception when retrying. Exception was: {0}. Will continue to retry.", ex.Message);
            }
        }

        /// <summary>
        /// Update the collection tracking blobs which were NOT successfully restored
        /// by creating a document containing the names of the blob file (and its container) which were NOT restored
        /// </summary>
        /// <param name="containerName">Blob Storage container name of the Blob that was NOT successfully restored</param>
        /// <param name="blobName">Blob file that was NOT restored successfully</param>
        /// <returns></returns>
        private async Task UpdateFailedRestoreBlob(string containerName, string blobName)
        {
            string restoreFailureDatabaseName = ConfigurationManager.AppSettings["RestoreFailureDatabaseName"];
            string restoreFailureCollectionName = ConfigurationManager.AppSettings["RestoreFailureCollectionName"];

            BlobRestoreStatusDocument blobRestoreFailureDocument = new BlobRestoreStatusDocument();
            blobRestoreFailureDocument.ContainerName = containerName;
            blobRestoreFailureDocument.BlobName = blobName;
            blobRestoreFailureDocument.Status = "failure";
            blobRestoreFailureDocument.Id = string.Concat(containerName, "-", blobName);

            try
            {
                await CosmosDBHelper.UpsertDocumentAsync(
                    this.DocumentClient,
                    restoreFailureDatabaseName,
                    restoreFailureCollectionName,
                    blobRestoreFailureDocument,
                    this.MaxRetriesOnDocumentClientExceptions);

                Console.WriteLine("Successfully updated RestoreFailureCollection with the blob: {0} in container: {1} which was NOT successfully restored", blobName, containerName);
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
                    string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreHelperDatabaseName"];
                    string restoreHelperCollectionName = ConfigurationManager.AppSettings["RestoreHelperCollectionName"];
                    Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreDatabaseName, restoreHelperCollectionName);

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
