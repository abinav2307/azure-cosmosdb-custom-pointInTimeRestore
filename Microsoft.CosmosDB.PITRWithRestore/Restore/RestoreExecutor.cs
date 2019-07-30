
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
    using Microsoft.CosmosDB.PITRWithRestore.Logger;

    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    using System;
    using System.Collections.Async;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
    using System.Linq;
    using System.Text;
    using System.Threading;
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

        /// <summary>
        /// For Epoch time conversions using midnight on January 1, 1970
        /// </summary>
        private static readonly DateTime epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// The collection to which the data will be restored
        /// </summary>
        private DocumentCollection RestoreCollection;

        /// <summary>
        /// Mapping of restored document count per blob container
        /// </summary>
        private ConcurrentDictionary<string, int> ContainerToDocCountRestoredMapper;

        /// <summary>
        /// List of Blob Containers to be restored
        /// </summary>
        private List<string> ContainersToRestore;

        /// <summary>
        /// List of containers to be restored by this instance of the 
        /// </summary>
        private List<string> ContainersToRestoreInParallel;

        /// <summary>
        /// Boolean flag keeping track of progress of restores to be executed by this instance
        /// </summary>
        private bool CompletedRestoringFromBlobContainers;

        /// <summary>
        /// Number of leases acquired on blob containers containing backups to be restored
        /// </summary>
        private long NumberOfBlobContainerLeasesAcquired;

        /// <summary>
        /// Logger to push messages during run time
        /// </summary>
        private ILogger Logger;

        /// <summary>
        /// /ActivityId generated as a Guid and used for logging to LogAnalytics
        /// </summary>
        private string GuidForLogAnalytics;

        /// <summary>
        /// Documents feed link to use for the Restore Collection
        /// </summary>
        private string DocumentsFeedLink;

        /// <summary>
        /// Documents feed link for the Restore Success Collection
        /// </summary>
        private string RestoreSuccessDocumentsFeedLink;

        /// <summary>
        /// Documents feed link for the Restore Failure Collection
        /// </summary>
        private string RestoreFailureDocumentsFeedLink;

        /// <summary>
        /// Documents feed link for the Restore Helper Collection
        /// </summary>
        private string RestoreHelperDocumentsFeedLink;

        public RestoreExecutor(DocumentClient documentClient, string hostName)
        {
            this.DocumentClient = documentClient;
            this.HostName = hostName;
            this.ContainersToRestore = new List<string>();
            this.ContainersToRestoreInParallel = new List<string>();
            this.ContainerToDocCountRestoredMapper = new ConcurrentDictionary<string, int>();
            this.CompletedRestoringFromBlobContainers = false;

            if (bool.Parse(ConfigurationManager.AppSettings["PushLogsToLogAnalytics"]))
            {
                this.Logger = new LogAnalyticsLogger();
            }
            else
            {
                this.Logger = new ConsoleLogger();
            }            

            // Generate a new GUID as an ActivityId for this specific execution of Change Feed
            this.GuidForLogAnalytics = Guid.NewGuid().ToString();

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
                throw new ArgumentException("The connection string for the Blob Storage Account is invalid. ");
            }

            // Creates 4 collections for the restore job
            // 1. RestoreCollection - collection into which the backups will be restored
            // 2. RestoreHelperCollection - Lease collection equivalent, keeping track of host names backing up specific Blob Storage containers
            // 3. RestoreSuccessCollection - collection tracking state of restore per backup containers. Status = "In Progress" or "Completed"
            // 4. RestoreFailureCollection - collection traking of failed blob files that could not be successfully restored
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
                restoreHelperCollectionPartitionKey,
                this.Logger,
                false).Wait();

            this.RestoreHelperDocumentsFeedLink = 
                UriFactory.CreateDocumentCollectionUri(restoreHelperDatabaseName, restoreHelperCollectionName).ToString();

            string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            string restoreCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];
            int restoreCollectionThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreCollectionThroughput"]);
            string restoreCollectionPartitionKey = ConfigurationManager.AppSettings["RestoreCollectionPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                restoreDatabaseName,
                restoreCollectionName,
                restoreCollectionThroughput,
                restoreCollectionPartitionKey,
                this.Logger,
                false).Wait();

            this.DocumentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreDatabaseName, restoreCollectionName).ToString();

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
                restoreSuccessCollectionPartitionKey,
                this.Logger,
                false).Wait();

            this.RestoreSuccessDocumentsFeedLink = 
                UriFactory.CreateDocumentCollectionUri(restoreSuccessDatabaseName, restoreSuccessCollectionName).ToString();

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
                restoreFailureCollectionPartitionKey,
                this.Logger,
                false).Wait();

            this.RestoreFailureDocumentsFeedLink = 
                UriFactory.CreateDocumentCollectionUri(restoreFailureDatabaseName, restoreFailureCollectionName).ToString();

            this.RestoreCollection = this.DocumentClient.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(restoreDatabaseName))
                .Where(c => c.Id == restoreCollectionName).AsEnumerable().FirstOrDefault();
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

                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 3: ActivityId: {0} - Start time for restore is: {1}",
                        this.GuidForLogAnalytics,
                        this.StartTimeForRestore.ToString("yyyy-MM-dd HH:mm:ss")));

            }
            catch (ArgumentNullException ex)
            {
                // If no specific start time is entered by the user, the restore will start from the beginning
                this.StartTimeForRestore = DateTime.MinValue;
            }
            catch (FormatException ex)
            {
                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 2: ActivityId: {0} - Invalid format for start time for restore. The correct format is: yyyy-MM-dd HH:mm:ss", 
                        this.GuidForLogAnalytics));

                throw new ArgumentException("Invalid format for start time for restore. The correct format is: yyyy-MM-dd HH:mm:ss");
            }

            try
            {
                string endTimeForRestoreAsString = ConfigurationManager.AppSettings["endTimeForRestore"];

                this.EndTimeForRestore = DateTime.ParseExact(endTimeForRestoreAsString, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 3: ActivityId: {0} - End time for restore is: {1}",
                        this.GuidForLogAnalytics,
                        this.EndTimeForRestore.ToString("yyyy-MM-dd HH:mm:ss")));
            }
            catch (ArgumentNullException ex)
            {
                // If no specific start time is entered by the user, the restore will start from the beginning
                this.EndTimeForRestore = DateTime.MaxValue;
            }
            catch (FormatException ex)
            {
                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 2: ActivityId: {0} - Invalid format for start time for restore. The correct format is: yyyy-MM-dd HH:mm:ss",
                        this.GuidForLogAnalytics));

                throw new ArgumentException("Invalid format for end time for restore. The correct format is: yyyy-MM-dd HH:mm:ss");
            }
        }

        private async Task RestoreDocuments()
        {
            Stopwatch totalExecutionTime = new Stopwatch();
            totalExecutionTime.Start();

            Parallel.ForEach(this.ContainersToRestoreInParallel, new ParallelOptions { MaxDegreeOfParallelism = 1 }, async (eachContainerToRestoreInParallel) =>
            {
                // 1. First, check for failed backups for this container and attempt to once again back them up prior to restoration
                //await this.BackupFailedBackups(eachContainerToRnew ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, estoreInParallel);

                CloudBlobContainer containerToRestore = this.CloudBlobClient.GetContainerReference(eachContainerToRestoreInParallel);

                Stopwatch containerContentStopWatch = new Stopwatch();
                containerContentStopWatch.Start();

                // 2. Get the list of blob files within the container(s) that satisfy the time windows for the restore
                List<string> listOfBlobsToRestore = this.GetListOfBlobsToRestore(eachContainerToRestoreInParallel);

                containerContentStopWatch.Start();

                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 3: ActivityId - {0} - Fetched {1} blobs for container: {2} in {3} seconds",
                        this.GuidForLogAnalytics,
                        listOfBlobsToRestore.Count, 
                        eachContainerToRestoreInParallel,
                        containerContentStopWatch.ElapsedMilliseconds / 1000));

                int maxDegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);

                Parallel.ForEach(listOfBlobsToRestore, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, async (blobFileToRestore) =>
                {
                    CloudBlockBlob blockBlob = containerToRestore.GetBlockBlobReference(blobFileToRestore);
                    blockBlob.FetchAttributes();

                    JArray jArray = null;
                    List<JObject> documentsFailedToRestore = new List<JObject>();
                    List<Exception> exceptionsCausingFailure = new List<Exception>();

                    if (bool.Parse(ConfigurationManager.AppSettings["UseCompression"]))
                    {
                        byte[] byteArray = new byte[blockBlob.Properties.Length];
                        blockBlob.DownloadToByteArray(byteArray, 0);

                        StringBuilder sB = new StringBuilder();
                        foreach (byte item in Decompress(byteArray))
                        {
                            sB.Append((char)item);
                        }

                        jArray = JsonConvert.DeserializeObject<JArray>(sB.ToString());
                    }
                    else
                    {
                        jArray = JsonConvert.DeserializeObject<JArray>(blockBlob.DownloadText());
                    }

                    await WriteJArrayToCosmosDB(jArray, this.GuidForLogAnalytics, documentsFailedToRestore, exceptionsCausingFailure);

                    // If the Blob was successfully restored, update the success/failure tracking collection with the name of the restored Blob
                    if (documentsFailedToRestore.Count == 0)
                    {
                        this.ContainerToDocCountRestoredMapper[eachContainerToRestoreInParallel] += jArray.Count;
                    }
                    else
                    {
                        this.Logger.WriteMessage(
                            string.Format(
                                "Sev 1: ActivityId - {0} - Failures encountered when restoring {1} documents from blob: {2} in {3} container",
                                this.GuidForLogAnalytics,
                                jArray.Count,
                                blobFileToRestore,
                                eachContainerToRestoreInParallel));

                        // Update documents that were not successfully restored to the specified Cosmos DB collection
                        await this.UpdateFailedRestoreBlob(
                            eachContainerToRestoreInParallel, 
                            blobFileToRestore, 
                            this.GuidForLogAnalytics,
                            documentsFailedToRestore,
                            exceptionsCausingFailure);
                    }
                });
            });
        }

        private async Task WriteStatusOfRestoreToCosmosDB(string containerName, string status)
        {
            string restoreFailureDatabaseName = ConfigurationManager.AppSettings["RestoreSuccessDatabaseName"];
            string restoreFailureCollectionName = ConfigurationManager.AppSettings["RestoreSuccessCollectionName"];

            try
            {
                BlobRestoreStatusDocument blobRestoreSuccessDocument = new BlobRestoreStatusDocument();
                blobRestoreSuccessDocument.ContainerName = containerName;
                blobRestoreSuccessDocument.Status = status;
                blobRestoreSuccessDocument.Id = string.Concat(containerName);
                blobRestoreSuccessDocument.DocumentCount = this.ContainerToDocCountRestoredMapper[containerName];

                await CosmosDBHelper.UpsertDocumentAsync(
                    this.DocumentClient,
                    this.RestoreSuccessDocumentsFeedLink,
                    blobRestoreSuccessDocument,
                    this.MaxRetriesOnDocumentClientExceptions,
                    this.GuidForLogAnalytics,
                    this.Logger);
            }
            catch (Exception ex)
            {
                this.Logger.WriteMessage(
                    string.Format(
                        "Caught Exception when retrying to update successfully restored document count. Exception was: {0}. Will continue to retry.", ex.Message));
            }
        }

        private async Task UpdateSuccessfullyRestoredDocumentCount(bool populateMetricsDuringRestore)
        {
            while (!this.CompletedRestoringFromBlobContainers && populateMetricsDuringRestore)
            {
                Thread.Sleep(60 * 1000);

                foreach (string containerToRestore in this.ContainersToRestoreInParallel)
                {
                    await this.WriteStatusOfRestoreToCosmosDB(containerToRestore, "In Progress");
                }
            }
        }

        /// <summary>
        /// Restores the previously backed up documents from the Blob Storage Account
        /// </summary>
        /// <returns></returns>
        public async Task ExecuteRestore()
        {
            this.ContainersToRestore = BlobStorageHelper.GetListOfContainersInStorageAccount(this.CloudBlobClient);
            int numberOfContainersToRestore = int.Parse(ConfigurationManager.AppSettings["MaxContainersToRestorePerWorker"]);

            int numAttempts = 0;
            while (this.NumberOfBlobContainerLeasesAcquired < this.ContainersToRestore.Count && 
                   numAttempts < this.MaxRetriesOnDocumentClientExceptions)
            {
                string containerToRestore = await this.GetNextContainerToRestore(this.ContainersToRestore);
                if (!string.IsNullOrEmpty(containerToRestore))
                {
                    this.ContainersToRestoreInParallel.Add(containerToRestore);
                    this.ContainerToDocCountRestoredMapper[containerToRestore] = 0;

                    if(numberOfContainersToRestore != -1 && this.ContainersToRestoreInParallel.Count >= numberOfContainersToRestore)
                    {
                        break;
                    }
                }
                else
                {
                    numAttempts++;
                }
            }

            Task producerTask = Task.Run(() => this.RestoreDocuments());
            Task consumerTask = Task.Run(() => this.UpdateSuccessfullyRestoredDocumentCount(true));

            await Task.WhenAll(producerTask, consumerTask);

            // Update the status to completed for all the containers being restored by this machine
            foreach (string containerToRestore in this.ContainersToRestoreInParallel)
            {
                await this.WriteStatusOfRestoreToCosmosDB(containerToRestore, "Completed");
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
                                if(bool.Parse(ConfigurationManager.AppSettings["UseCompression"]))
                                {
                                    BlobStorageHelper.WriteToBlobStorage(
                                        blockBlob,
                                        compressedByteArray,
                                        this.MaxRetriesOnDocumentClientExceptions);
                                }
                                else
                                {
                                    StringBuilder decompressedJArrayOfDocuments = new StringBuilder(compressedByteArray.Length);
                                    foreach (byte item in Decompress(compressedByteArray))
                                    {
                                        decompressedJArrayOfDocuments.Append((char)item);
                                    }

                                    BlobStorageHelper.WriteStringToBlobStorage(
                                        blockBlob,
                                        decompressedJArrayOfDocuments.ToString(),
                                        this.MaxRetriesOnDocumentClientExceptions);
                                }                                

                                // Upon successfully backing up the previously failed backup,
                                // delete the document from the 'BackupFailureCollection'
                                await CosmosDBHelper.DeleteDocmentAsync(
                                    this.DocumentClient,
                                    backupFailureDatabaseName,
                                    backupFailureCollectionName,
                                    fileName,
                                    fileName,
                                    this.MaxRetriesOnDocumentClientExceptions,
                                    this.GuidForLogAnalytics,
                                    this.Logger);
                            }
                            catch (Exception ex)
                            {
                                this.Logger.WriteMessage(
                                    string.Format(
                                        "Sev 2: Exception while attempting to backup previously failed backups: {0}. Stack trace is: {1}",
                                        ex.Message,
                                        ex.StackTrace));
                            }
                        }                        
                    }
                }
            }
            catch (Exception ex)
            {
                this.Logger.WriteMessage(
                    string.Format(
                        "Sev 2: Exception message while querying for previously failed backups: {0}. Stack trace is: {1}",
                        ex.Message,
                        ex.StackTrace));
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
        private async Task<List<JObject>> WriteJArrayToCosmosDB(
            JArray jArray, 
            string activityIdForBlobRestore, 
            List<JObject> documentsFailedToRestore,
            List<Exception> exceptionsCausingFailure)
        {
            string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            string restoreCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(restoreDatabaseName, restoreCollectionName);

            foreach (JObject eachJObject in jArray)
            {
                Exception exToLog = null;

                if (VerifyTimeRangeForDocumentRestore(eachJObject))
                {
                    try
                    {
                        bool isUpsertSuccessful = await CosmosDBHelper.UpsertDocumentAsync(
                            this.DocumentClient,
                            this.DocumentsFeedLink, 
                            eachJObject, 
                            this.MaxRetriesOnDocumentClientExceptions,
                            activityIdForBlobRestore,
                            this.Logger,
                            exToLog);

                        if(!isUpsertSuccessful)
                        {
                            documentsFailedToRestore.Add(eachJObject);
                            exceptionsCausingFailure.Add(exToLog);

                            this.Logger.WriteMessage(
                               string.Format(
                                   "Sev 1: ActivityId - {0} - Document could not be restored.",
                                   activityIdForBlobRestore));
                        }
                    }
                    catch (Exception e)
                    {
                        this.Logger.WriteMessage(
                           string.Format(
                               "Sev 1: ActivityId - {0} - Caught exception when attempting to upsert document. Original exception message was: {1}. Stack trace is: {2}",
                               activityIdForBlobRestore,
                               e.Message,
                               e.StackTrace));

                        documentsFailedToRestore.Add(eachJObject);
                        exceptionsCausingFailure.Add(e);
                    }
                }
                else
                {
                    DateTime timestampInDocument = epoch.AddSeconds((long)eachJObject["_ts"]);

                    this.Logger.WriteMessage(
                           string.Format(
                               "Sev 3: ActivityId - {0} - Document is not in restore window. Timestamp for document is {1}. Restore start time = {2} and Restore end time = {3}",
                               activityIdForBlobRestore,
                               timestampInDocument.ToString("yyyyMMdd HH: mm:ss"),
                               this.StartTimeForRestore.ToString("yyyyMMdd HH: mm:ss"),
                               this.EndTimeForRestore.ToString("yyyyMMdd HH: mm:ss")));
                }
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
            DateTime timestampInDocument = epoch.AddSeconds((long)jObject["_ts"]);

            if (DateTime.Compare(timestampInDocument, this.StartTimeForRestore) >= 0 && DateTime.Compare(timestampInDocument, this.EndTimeForRestore) <= 0)
            {
                isDocumentInRestoreWindow = true;
            }

            return isDocumentInRestoreWindow;
        }

        /// <summary>
        /// Update the collection tracking blobs which were NOT successfully restored
        /// by creating a document containing the names of the blob file (and its container) which were NOT restored
        /// </summary>
        /// <param name="containerName">Blob Storage container name of the Blob that was NOT successfully restored</param>
        /// <param name="blobName">Blob file that was NOT restored successfully</param>
        /// <returns></returns>
        private async Task UpdateFailedRestoreBlob(
            string containerName, 
            string blobName, 
            string activityIdForRestore, 
            List<JObject> documentsFailedToRestore,
            List<Exception> exceptionsCausingFailure)
        {
            string restoreFailureDatabaseName = ConfigurationManager.AppSettings["RestoreFailureDatabaseName"];
            string restoreFailureCollectionName = ConfigurationManager.AppSettings["RestoreFailureCollectionName"];

            for (int countOfFailures = 0; countOfFailures < documentsFailedToRestore.Count; countOfFailures++)
            {
                BlobRestoreStatusDocument blobRestoreFailureDocument = new BlobRestoreStatusDocument();
                blobRestoreFailureDocument.ContainerName = containerName;
                blobRestoreFailureDocument.BlobName = blobName;
                blobRestoreFailureDocument.Status = "failure";
                blobRestoreFailureDocument.Id = string.Concat(containerName, "-", blobName);

                blobRestoreFailureDocument.DocumentFailedToRestore = documentsFailedToRestore[countOfFailures];

                if (exceptionsCausingFailure[countOfFailures] != null)
                {
                    blobRestoreFailureDocument.ExceptionMessage = exceptionsCausingFailure[countOfFailures].Message;
                    blobRestoreFailureDocument.ExceptionType = exceptionsCausingFailure[countOfFailures].GetType().ToString();

                    if (exceptionsCausingFailure[countOfFailures].InnerException != null)
                    {
                        blobRestoreFailureDocument.InnerExceptionMessage = exceptionsCausingFailure[countOfFailures].InnerException.Message;
                    }
                }
                
                try
                {
                    await CosmosDBHelper.UpsertDocumentAsync(
                        this.DocumentClient,
                        this.RestoreFailureDocumentsFeedLink,
                        blobRestoreFailureDocument,
                        this.MaxRetriesOnDocumentClientExceptions,
                        this.GuidForLogAnalytics,
                        this.Logger);
                }
                catch (Exception ex)
                {
                    this.Logger.WriteMessage(
                        string.Format(
                            "Sev 2: ActivityId - {0} - Caught Exception when updating RestoreFailureCollection with the blob: {1} in container: {2} which was NOT successfully restored. Exception was: {3}. Will continue to retry",
                            activityIdForRestore,
                            blobName,
                            containerName,
                            ex.Message));
                }
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
                }
                else if (DateTime.Compare(StartTimeForRestore, startTimeInBlobFile) <= 0 && DateTime.Compare(EndTimeForRestore, startTimeInBlobFile) >= 0)
                {
                    blobsToRestore.Add(blobName);
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
            this.NumberOfBlobContainerLeasesAcquired = await this.GetDocumentCountInBlobStorageLeaseCollection();

            do
            {
                // If leases have been acquired for all containers in the Blob Storage account, there are no more containers to process for restore
                if (this.NumberOfBlobContainerLeasesAcquired < numberOfContainersToRestore)
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
                                await this.DocumentClient.CreateDocumentAsync(documentsFeedLink, restoreContainerDocument, null, true);
                                success = true;
                                containerName = eachContainerToRestore;

                                this.Logger.WriteMessage(
                                    string.Format(
                                        "Sev 3: ActivityId - {0} - Successfully tracking container: {1}",
                                        this.GuidForLogAnalytics,
                                        containerName));
                            }
                            catch (DocumentClientException ex)
                            {
                                if ((int)ex.StatusCode != 409)
                                {
                                    this.Logger.WriteMessage(
                                        string.Format(
                                            "Sev 2: ActivityId - {0} - Other exception thrown when attempting to pick up a least on a container. Status code = {1}",
                                            this.GuidForLogAnalytics,
                                            (int)ex.StatusCode));
                                }
                            }
                            catch (Exception ex)
                            {
                                this.Logger.WriteMessage(
                                    string.Format(
                                        "Sev 2: ActivityId - {0} - Exception thrown when attempting to pick up a lease on a container. Original exception was: {1}",
                                        this.GuidForLogAnalytics,
                                        ex.Message));
                            }
                        }
                    }

                    // Refresh count of leases which have been picked up for the Blob Storage containers to restore
                    this.NumberOfBlobContainerLeasesAcquired = await this.GetDocumentCountInBlobStorageLeaseCollection();

                    numRetries++;
                }
                else
                {
                    success = true;
                }

            } while (!success && numRetries <= this.MaxRetriesOnDocumentClientExceptions);

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
