
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
    using System.Linq;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Diagnostics;
    using System.Globalization;
    using System.IO;
    using System.IO.Compression;
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
        /// Mapping of blobs successfully restored per blob container
        /// </summary>
        private ConcurrentDictionary<string, int> ContainerToBlobCountRestoredMapper;

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
        /// Name of the database with the container storing successes, failures and leases for the Restore job
        /// </summary>
        private string RestoreStatusDatabaseName;

        /// <summary>
        /// Name of the container storing successes, failures and leases for the Restore job
        /// </summary>
        private string RestoreStatusContainerName;

        /// <summary>
        /// Name of the database into which the backups will be restored
        /// </summary>
        private string RestoreDatabaseName;

        /// <summary>
        /// Name of the container into which the backups will be restored
        /// </summary>
        private string RestoreContainerName;

        /// <summary>
        /// The field representing the partition key for the documents to be restored
        /// </summary>
        private string RestoreContainerPartitionKey;

        /// <summary>
        /// Documents feed link for the Restore Status Container
        /// </summary>
        private string RestoreStatusDocumentsFeedLink;

        public RestoreExecutor(DocumentClient documentClient, string hostName)
        {
            this.DocumentClient = documentClient;
            this.HostName = hostName;
            this.ContainersToRestore = new List<string>();
            this.ContainersToRestoreInParallel = new List<string>();
            this.ContainerToBlobCountRestoredMapper = new ConcurrentDictionary<string, int>();
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
            string storageConnectionString = ConfigurationManager.AppSettings["BlobStorageConnectionString"];

            // Verify the Blob Storage Connection String
            if (CloudStorageAccount.TryParse(storageConnectionString, out this.StorageAccount))
            {
                this.CloudBlobClient = this.StorageAccount.CreateCloudBlobClient();
            }
            else
            {
                throw new ArgumentException("The connection string for the Blob Storage Account is invalid. ");
            }

            // Creates 2 containers for the restore job
            // 1. RestoreCollection - container into which the backups will be restored
            // 2. RestoreStatusContainer - container traking of failed blob files that could not be successfully restored
            this.CreateContainersForRestore();

            this.ExtractStartAndEndTimeForRestore();
        }

        /// <summary>
        /// Create the following containers if they don't already exist:
        /// 1. RestoreContainer       - Cosmos DB container into which backup blobs will be restored
        /// 2. RestoreStatusContainer - Cosmos DB container tracking restore successes, failures as well as assignment
        ///                             of leases for blob containers (corresponding to backups for physical partitions) 
        ///                             to be restored
        /// </summary>
        private void CreateContainersForRestore()
        {
            // 1. Create the container into which the backups will be restored
            this.RestoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            this.RestoreContainerName = ConfigurationManager.AppSettings["RestoreContainerName"];
            int restoreContainerThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreContainerThroughput"]);
            string restoreContainerPartitionKey = ConfigurationManager.AppSettings["RestoreContainerPartitionKey"];
            this.RestoreContainerPartitionKey = restoreContainerPartitionKey.Substring(1);

            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                this.RestoreDatabaseName,
                this.RestoreContainerName,
                restoreContainerThroughput,
                restoreContainerPartitionKey,
                this.Logger,
                false).Wait();

            this.DocumentsFeedLink = UriFactory.CreateDocumentCollectionUri(this.RestoreDatabaseName, this.RestoreContainerName).ToString();

            // 2. Create the container tracking successes, failures and leases for the restore process
            this.RestoreStatusDatabaseName = ConfigurationManager.AppSettings["RestoreStatusDatabaseName"];
            this.RestoreStatusContainerName = ConfigurationManager.AppSettings["RestoreStatusContainerName"];
            int restoreStatusContainerThroughput = int.Parse(ConfigurationManager.AppSettings["RestoreStatusContainerThroughput"]);
            string restoreStatusContainerPartitionKey = ConfigurationManager.AppSettings["RestoreStatusContainerPartitionKey"];
            CosmosDBHelper.CreateCollectionIfNotExistsAsync(
                this.DocumentClient,
                this.RestoreStatusDatabaseName,
                this.RestoreStatusContainerName,
                restoreStatusContainerThroughput,
                restoreStatusContainerPartitionKey,
                this.Logger,
                false).Wait();

            this.RestoreStatusDocumentsFeedLink =
                UriFactory.CreateDocumentCollectionUri(this.RestoreStatusDatabaseName, this.RestoreStatusContainerName).ToString();
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

        /// <summary>
        /// Restores the previously backed up documents from the Blob Storage Account
        /// </summary>
        /// <returns></returns>
        public async Task ExecuteRestore()
        {
            // Fetch the list of backup containers from the Blob Storage account to be restored
            this.ContainersToRestore = BlobStorageHelper.GetListOfContainersInStorageAccount(this.CloudBlobClient);

            // Determine the list of backup containers yet to complete restoration
            long countOfBackupContainersUnfinished = await this.GetCountOfBackupContainersUnfinished();
            do
            {
                this.ContainersToRestoreInParallel = new List<string>();

                // Retrieve the count of successfully restored blobs in each backup container
                foreach (string eachContainerToRestore in this.ContainersToRestore)
                {
                    int countOfRestoredBlobsInContainer = await this.GetCountOfBlobsPreviouslyRestoredForContainer(eachContainerToRestore);
                    this.ContainerToBlobCountRestoredMapper[eachContainerToRestore] = countOfRestoredBlobsInContainer;
                }

                // Fetch the max number of containers to be restored by a single client
                int maxContainersToRestore = int.Parse(ConfigurationManager.AppSettings["MaxContainersToRestorePerWorker"]);

                do
                {
                    // Delete all leases that are more than 5 minutes old
                    // (Since we update the count of successful restores every minute, any lease that is over 5 minutes old is due to machine failure)
                    await this.DeleteExpiredLeasesOnBackupContainersToBeRestored();

                    int numAttempts = 0;
                    while (this.NumberOfBlobContainerLeasesAcquired < this.ContainersToRestore.Count &&
                           numAttempts < this.MaxRetriesOnDocumentClientExceptions)
                    {
                        string containerToRestore = await this.GetNextContainerToRestore(this.ContainersToRestore);
                        if (!string.IsNullOrEmpty(containerToRestore))
                        {
                            numAttempts = 0;
                            this.ContainersToRestoreInParallel.Add(containerToRestore);
                            this.ContainerToBlobCountRestoredMapper[containerToRestore] = 0;

                            if (maxContainersToRestore != -1 && this.ContainersToRestoreInParallel.Count >= maxContainersToRestore)
                            {
                                break;
                            }
                        }
                        else
                        {
                            numAttempts++;
                            this.Logger.WriteMessage(string.Format("Current count of acquired leases = {0}. Sleeping for 5 seconds", this.NumberOfBlobContainerLeasesAcquired));
                            Thread.Sleep(5 * 1000);
                        }
                    }

                    countOfBackupContainersUnfinished = await this.GetCountOfBackupContainersUnfinished();

                } while (this.ContainersToRestoreInParallel.Count == 0 && countOfBackupContainersUnfinished > 0);

                Task producerTask = Task.Run(() => this.RestoreDocuments());
                Task consumerTask = Task.Run(() => this.UpdateSuccessfullyRestoredDocumentCount(true));

                await Task.WhenAll(producerTask, consumerTask);

                // Update the status to "Completed" for all the containers being restored by this machine
                foreach (string containerToRestore in this.ContainersToRestoreInParallel)
                {
                    await this.WriteStatusOfRestoreToCosmosDB(containerToRestore, "Completed");
                }

                countOfBackupContainersUnfinished = await this.GetCountOfBackupContainersUnfinished();

            } while (countOfBackupContainersUnfinished > 0);
        }

        /// <summary>
        /// Retrieves the list of backup containers that are still being restored
        /// 
        /// If the restore job is running for the first time, this method returns the number of containers 
        /// retrieved from the Blob Storage account containing the backups
        /// </summary>
        /// <returns></returns>
        private async Task<long> GetCountOfBackupContainersUnfinished()
        {
            int countOfBackupContainersUnfinished = 0;
            string queryString = string.Format("select * from c where c.documentType = 'Restore Helper'");

            List<Document> previouslySucceededRestoresForContainer = await CosmosDBHelper.QueryDocuments(
                this.DocumentClient,
                this.RestoreStatusDatabaseName,
                this.RestoreStatusContainerName,
                queryString,
                this.MaxRetriesOnDocumentClientExceptions,
                this.GuidForLogAnalytics,
                this.Logger);

            if (previouslySucceededRestoresForContainer.Count == 0)
            {
                countOfBackupContainersUnfinished = this.ContainersToRestore.Count;
            }
            else
            {
                queryString = string.Format("select * from c where c.documentType = 'Restore Helper' and c.status != 'Completed'");

                previouslySucceededRestoresForContainer = await CosmosDBHelper.QueryDocuments(
                    this.DocumentClient,
                    this.RestoreStatusDatabaseName,
                    this.RestoreStatusContainerName,
                    queryString,
                    this.MaxRetriesOnDocumentClientExceptions,
                    this.GuidForLogAnalytics,
                    this.Logger);

                foreach (Document eachDocument in previouslySucceededRestoresForContainer)
                {
                    countOfBackupContainersUnfinished++;
                }
            }

            return countOfBackupContainersUnfinished;
        }

        /// <summary>
        /// Deletes all expired leases on containers yet to be fully restored
        /// (most likely due to machines dying or restarting)
        /// </summary>
        private async Task DeleteExpiredLeasesOnBackupContainersToBeRestored()
        {
            // Calculate the epoch time as of 3 minutes prior to current execution.
            // All leases older than the calculated time should be treated as expired leases to be deleted and eligible for renewal
            TimeSpan t = DateTime.UtcNow.AddMinutes(-2) - new DateTime(1970, 1, 1);
            int secondsSinceEpochForExpiredLeases = (int)t.TotalSeconds;

            string queryStringForExpiredRestoreLeases =
                string.Format("select * from c where c.documentType = 'Restore Helper' and c.status in ('In Progress', 'Initializing') and c._ts <= {0}", secondsSinceEpochForExpiredLeases);

            List<Document> containersWithExpiredLeasesForRestore =
                await CosmosDBHelper.QueryDocuments(
                    this.DocumentClient,
                    this.RestoreStatusDatabaseName,
                    this.RestoreStatusContainerName,
                    queryStringForExpiredRestoreLeases,
                    this.MaxRetriesOnDocumentClientExceptions,
                    this.GuidForLogAnalytics,
                    this.Logger);

            // Delete each document with an expired lease
            foreach (Document eachDocumentWithAnExpiredRestoreLease in containersWithExpiredLeasesForRestore)
            {
                string restoreStatusPartitionKeyProperty = ConfigurationManager.AppSettings["RestoreStatusContainerPartitionKey"];
                if (restoreStatusPartitionKeyProperty[0] == '/')
                {
                    restoreStatusPartitionKeyProperty = restoreStatusPartitionKeyProperty.Remove(0, 1);
                }

                string partitionKey = eachDocumentWithAnExpiredRestoreLease.GetPropertyValue<string>(restoreStatusPartitionKeyProperty);

                string id = eachDocumentWithAnExpiredRestoreLease.GetPropertyValue<string>("id");

                await CosmosDBHelper.DeleteDocmentAsync(
                    this.DocumentClient,
                    this.RestoreStatusDatabaseName,
                    this.RestoreStatusContainerName,
                    partitionKey,
                    id,
                    this.MaxRetriesOnDocumentClientExceptions,
                    this.GuidForLogAnalytics,
                    this.Logger);

                this.Logger.WriteMessage(string.Format("Sev 3: Removed expired lease on backup container document with partition key: {0} and id: {1}", partitionKey, id));
            }

            // Number of containers in the Blob Storage account with leases acquired
            this.NumberOfBlobContainerLeasesAcquired = await this.GetDocumentCountInBlobStorageLeaseCollection();
        }

        /// <summary>
        /// Updates the count of restored document for each blob storage container
        /// </summary>
        /// <param name="populateMetricsDuringRestore"></param>
        /// <returns></returns>
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

        private async Task RestoreDocuments()
        {
            Stopwatch totalExecutionTime = new Stopwatch();
            totalExecutionTime.Start();

            int maxDegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);
            bool workloadHasUpdates = bool.Parse(ConfigurationManager.AppSettings["HasUpdates"]);
            if (workloadHasUpdates)
            {
                maxDegreeOfParallelism = 1;
            }

            Parallel.ForEach(this.ContainersToRestoreInParallel, new ParallelOptions { MaxDegreeOfParallelism = 1 }, async (eachContainerToRestoreInParallel) =>
            {
                // 1. First, check for failed backups for this container and attempt to once again back them up prior to restoration
                //await this.BackupFailedBackups(eachContainerToRestore, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, estoreInParallel);

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
                        this.ContainerToBlobCountRestoredMapper[eachContainerToRestoreInParallel]++;
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

            this.CompletedRestoringFromBlobContainers = true;
            this.Logger.WriteMessage("Set completed status to true");
        }

        /// <summary>
        /// Write the count of documents successfully restored
        /// </summary>
        /// <param name="containerName">Blob Storage Container name being restored</param>
        /// <param name="status">Status of the Restore - 'In Progress' or 'Completed'</param>
        /// <returns></returns>
        private async Task WriteStatusOfRestoreToCosmosDB(string containerName, string status)
        {
            try
            {
                ResourceResponse<Document> blobRestoreStatusDocument = await CosmosDBHelper.ReadDocmentAsync(
                    this.DocumentClient,
                    this.RestoreStatusDatabaseName,
                    this.RestoreStatusContainerName,
                    containerName,
                    containerName,
                    this.MaxRetriesOnDocumentClientExceptions,
                    this.GuidForLogAnalytics,
                    this.Logger);

                if (blobRestoreStatusDocument != null)
                {
                    blobRestoreStatusDocument.Resource.SetPropertyValue("successfullyRestoredBlobCount", this.ContainerToBlobCountRestoredMapper[containerName]);
                    blobRestoreStatusDocument.Resource.SetPropertyValue("status", status);

                    this.Logger.WriteMessage(string.Format("Blob restore status document is: {0}", blobRestoreStatusDocument.Resource.ToString()));

                    await CosmosDBHelper.UpsertDocumentAsync(
                        this.DocumentClient,
                        this.RestoreStatusDocumentsFeedLink,
                        blobRestoreStatusDocument.Resource,
                        this.MaxRetriesOnDocumentClientExceptions,
                        this.GuidForLogAnalytics,
                        this.Logger);
                }
            }
            catch (Exception ex)
            {
                this.Logger.WriteMessage(
                    string.Format(
                        "Caught Exception when retrying to update successfully restored document count. Exception was: {0}. Will continue to retry.", ex.Message));
            }
        }

        /// <summary>
        /// Once again attempt to backup failed backups for the container being restored
        /// </summary>
        /// <param name="containerToRestore">Name of the Blob Storage container containing the backups to be restored</param>
        /// <returns></returns>
        private async Task BackupFailedBackups(string containerToRestore)
        {
            CloudBlobContainer cloudBlobContainer = this.CloudBlobClient.GetContainerReference(containerToRestore);
            cloudBlobContainer.CreateIfNotExists();

            string backupFailureDatabaseName = ConfigurationManager.AppSettings["BackupStatusDatabaseName"];
            string backupFailureContainerName = ConfigurationManager.AppSettings["BackupStatusContainerName"];

            string queryToExecute = string.Format(
                "select * from c where c.containerName = '{0}' and c.documentType = 'Failure'",
                containerToRestore);

            string collectionLink = UriFactory.CreateDocumentCollectionUri(
                backupFailureDatabaseName, backupFailureContainerName).ToString();

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
                        if (eachFailedBackupDocument.ExceptionType != null)
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
                                if (bool.Parse(ConfigurationManager.AppSettings["UseCompression"]))
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
                                    backupFailureContainerName,
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
        /// Retrieves the number of 'Restore Helper' documents in the RestoreStatusContainer.
        /// This container stores 1 lease document for every container in the Blob Storage Account.
        /// If the count of 'Restore Helper' documents equals the number of containers in the Blob Storage account, 
        /// there are no additional containers to restore to the specified Cosmos DB Restore container.
        /// </summary>
        /// <returns></returns>
        private async Task<long> GetDocumentCountInBlobStorageLeaseCollection()
        {
            long documentCount = 0;

            string queryToExecute = string.Format("select * from c where c.documentType = 'Restore Helper'");
            string collectionLink = this.RestoreStatusDocumentsFeedLink;
            try
            {
                var query = this.DocumentClient.CreateDocumentQuery<Document>(collectionLink,
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
                    documentCount += result.Count;
                }
            }
            catch (DocumentClientException ex)
            {
                if ((int)ex.StatusCode == 429)
                {
                    int numRetries = 0;
                    bool success = false;

                    while (numRetries < this.MaxRetriesOnDocumentClientExceptions && !success)
                    {
                        try
                        {
                            var query = this.DocumentClient.CreateDocumentQuery<Document>(collectionLink,
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
                                documentCount += result.Count;
                            }

                            success = true;
                        }
                        catch (DocumentClientException idex)
                        {
                            numRetries++;
                        }
                        catch (Exception ie)
                        {
                            numRetries++;
                        }
                    }
                }
            }
            catch (Exception e)
            {

            }

            return documentCount;
        }

        /// <summary>
        /// Restore each JObject in the input JArray into the specified Cosmos DB container
        /// and store the documents that were not successfully restored into the container tracking restore failures
        /// </summary>
        /// <param name="jArray"></param>
        /// <returns></returns>
        private async Task<List<JObject>> WriteJArrayToCosmosDB(
            JArray jArray,
            string activityIdForBlobRestore,
            List<JObject> documentsFailedToRestore,
            List<Exception> exceptionsCausingFailure)
        {
            foreach (JObject eachJObject in jArray)
            {
                Exception exToLog = null;

                if (VerifyTimeRangeForDocumentRestore(eachJObject))
                {
                    try
                    {
                        if (!IsDelete(eachJObject))
                        {
                            eachJObject.Remove("_metadata");

                            bool isUpsertSuccessful = await CosmosDBHelper.UpsertDocumentAsync(
                                this.DocumentClient,
                                this.DocumentsFeedLink,
                                eachJObject,
                                this.MaxRetriesOnDocumentClientExceptions,
                                activityIdForBlobRestore,
                                this.Logger,
                                exToLog);

                            if (!isUpsertSuccessful)
                            {
                                documentsFailedToRestore.Add(eachJObject);
                                exceptionsCausingFailure.Add(exToLog);

                                this.Logger.WriteMessage(
                                   string.Format(
                                       "Sev 1: ActivityId - {0} - Document could not be restored.",
                                       activityIdForBlobRestore));
                            }
                        }
                        else
                        {
                            JObject objectToDelete = GetPreviousImage(eachJObject);

                            bool isDeleteSuccessful = await CosmosDBHelper.DeleteDocmentAsync(
                                this.DocumentClient,
                                this.RestoreDatabaseName,
                                this.RestoreContainerName,
                                (string)eachJObject[this.RestoreContainerPartitionKey],
                                (string)eachJObject["id"],
                                this.MaxRetriesOnDocumentClientExceptions,
                                activityIdForBlobRestore,
                                this.Logger);

                            if (!isDeleteSuccessful)
                            {
                                documentsFailedToRestore.Add(eachJObject);
                                exceptionsCausingFailure.Add(exToLog);

                                this.Logger.WriteMessage(
                                   string.Format(
                                       "Sev 1: ActivityId - {0} - Document could not be restored.",
                                       activityIdForBlobRestore));
                            }
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
        /// In the case of a backup containing a deleted document, the current version will not exist
        /// Thus, the document to be deleted must be fetched from the "previousImage" object 
        /// embedded within the "_metadata" component of the archived JObject
        /// </summary>
        /// <param name="jObject">JObject representing the archived backup</param>
        /// <returns></returns>
        private JObject GetPreviousImage(JObject jObject)
        {
            JObject previousImage = null;

            if (jObject.GetValue("_metadata") != null)
            {
                JObject metadata = JObject.FromObject(jObject.GetValue("_metadata"));
                if (metadata.GetValue("previousImage") != null)
                {
                    previousImage = JObject.FromObject(metadata.GetValue("previousImage"));
                }
            }

            return previousImage;
        }

        /// <summary>
        /// If OpLog is used during the backup process, deletes will also be captured
        /// This method looks into the "metadata" of the backup to determine if the backup corresponds to a delete operation
        /// </summary>
        /// <param name="jObject">Each JObject archived during the backup process</param>
        /// <returns></returns>
        private bool IsDelete(JObject jObject)
        {
            bool isDelete = false;

            if (jObject.GetValue("_metadata") != null)
            {
                JObject metadata = JObject.FromObject(jObject.GetValue("_metadata"));
                if (metadata.GetValue("operationType") != null)
                {
                    if (metadata.GetValue("operationType").Equals("delete"))
                    {
                        isDelete = true;
                    }
                }
            }

            return isDelete;
        }

        /// <summary>
        /// Determines if the previously backed up document, is in the time window specified by the user for a restore
        /// </summary>
        /// <param name="jObject">JSON document previously backed up to Blob Storage</param>
        /// <returns></returns>
        private bool VerifyTimeRangeForDocumentRestore(JObject jObject)
        {
            bool isDocumentInRestoreWindow = false;

            if (jObject.GetValue("_ts") != null)
            {
                // Convert the _ts property of the document to restore, to a timestamp
                DateTime timestampInDocument = epoch.AddSeconds((long)jObject["_ts"]);

                if (DateTime.Compare(timestampInDocument, this.StartTimeForRestore) >= 0 && DateTime.Compare(timestampInDocument, this.EndTimeForRestore) <= 0)
                {
                    isDocumentInRestoreWindow = true;
                }
            }
            else
            {
                isDocumentInRestoreWindow = true;
            }

            return isDocumentInRestoreWindow;
        }

        /// <summary>
        /// Update the container tracking blobs which were NOT successfully restored
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
            for (int countOfFailures = 0; countOfFailures < documentsFailedToRestore.Count; countOfFailures++)
            {
                BlobRestoreStatusDocument blobRestoreFailureDocument = new BlobRestoreStatusDocument();
                blobRestoreFailureDocument.ContainerName = containerName;
                blobRestoreFailureDocument.BlobName = blobName;
                blobRestoreFailureDocument.DocumentType = "Failure";
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
                        this.RestoreStatusDocumentsFeedLink,
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
            IEnumerable<IListBlobItem> enumerable = blobContainer.ListBlobs();

            // Retrieve the count of blobs previously restored for this backup container
            int countOfPreviouslyRestoredBlobsForContainer = 0;
            if(this.ContainerToBlobCountRestoredMapper.ContainsKey(containerName))
            {
                countOfPreviouslyRestoredBlobsForContainer = this.ContainerToBlobCountRestoredMapper[containerName];
                this.Logger.WriteMessage(string.Format("Found {0} blobs previously restored for this container.", countOfPreviouslyRestoredBlobsForContainer));
            }

            List<IListBlobItem> blobsToRestoreForContainer = blobContainer.ListBlobs().ToList().OrderBy(x => ((CloudBlockBlob)x).Properties.LastModified).ToList();
            this.Logger.WriteMessage(string.Format("Retrieved {0} blobs containing backups.", blobsToRestoreForContainer.Count));

            blobsToRestoreForContainer.RemoveRange(0, countOfPreviouslyRestoredBlobsForContainer);
            this.Logger.WriteMessage(string.Format("After removing restored blobs. Blobs to restore = {0}", blobsToRestoreForContainer.Count));

            foreach (IListBlobItem blobItem in blobsToRestoreForContainer)
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
        /// Retrieves the count of previously restored blobs containing backups for the specified container
        /// </summary>
        /// <param name="containerName">Blob Storage container with backups that may have been partially restored on a prior run</param>
        /// <returns></returns>
        private async Task<int> GetCountOfBlobsPreviouslyRestoredForContainer(string containerName)
        {
            int countOfPreviouslyRestoredBlobs = 0;
            string queryString = string.Format("select * from c where c.id = '{0}' and c.documentType = 'Restore Helper'", containerName);

            List<Document> previouslySucceededRestoresForContainer = await CosmosDBHelper.QueryDocuments(
                this.DocumentClient, 
                this.RestoreStatusDatabaseName, 
                this.RestoreStatusContainerName, 
                queryString, 
                this.MaxRetriesOnDocumentClientExceptions, 
                this.GuidForLogAnalytics, 
                this.Logger);

            foreach (Document eachDocument in previouslySucceededRestoresForContainer)
            {
                countOfPreviouslyRestoredBlobs += eachDocument.GetPropertyValue<int>("successfullyRestoredBlobCount");
            }

            return countOfPreviouslyRestoredBlobs;
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
                    foreach (string eachContainerToRestore in shuffledContainerNames)
                    {
                        if (!success)
                        {
                            // Verify the container has not already been picked up by another AppService instance
                            // by attempting to create a document in the HelperCollection
                            BlobRestoreStatusDocument restoreContainerDocument = new BlobRestoreStatusDocument();
                            restoreContainerDocument.RestoreHostName = this.HostName;
                            restoreContainerDocument.Id = eachContainerToRestore;
                            restoreContainerDocument.ContainerName = eachContainerToRestore;
                            restoreContainerDocument.DocumentType = "Restore Helper";
                            restoreContainerDocument.Status = "Initializing";

                            try
                            {
                                await this.DocumentClient.CreateDocumentAsync(this.RestoreStatusDocumentsFeedLink, restoreContainerDocument, null, true);
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
                                            "Sev 2: ActivityId - {0} - Other exception thrown when attempting to pick up a lease on a container. Status code = {1}",
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
