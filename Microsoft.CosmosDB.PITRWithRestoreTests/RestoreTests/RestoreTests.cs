
namespace Microsoft.CosmosDB.PITRWithRestoreTests.RestoreTests
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.Client;
    using Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Microsoft.CosmosDB.PITRWithRestore.Backup;
    using Microsoft.CosmosDB.PITRWithRestore.Restore;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.CosmosDB.PITRWithRestore.BlobStorage;
    using Microsoft.CosmosDB.PITRWithRestore.CosmosDB;

    [TestClass]
    public class RestoreTests
    {
        /// <summary>
        /// Instance of the DocumentClient, used to push failed batches of backups to the Cosmos DB collection
        /// tracking failures during the backup process.
        /// </summary>
        private DocumentClient client = null;

        /// <summary>
        /// CloudStorageAccount instance retrieved after successfully establishing a connection to the specified Blob Storage Account,
        /// into which created and updated documents will be backed up
        /// </summary>
        private CloudStorageAccount StorageAccount;

        /// <summary>
        /// CloudBlobClient instance used to push backups to the specified Blob Storage Account
        /// </summary>
        private CloudBlobClient CloudBlobClient;

        /// <summary>
        /// Initialation step for backup tests:
        /// 1. Re-create the collection to be backed up to the specified Blob Storage Account
        /// 2. Cleanup the Blob Storage containers with backups (if any) from previous test executions
        /// </summary>
        [TestInitialize]
        public void TestSuiteSetUp()
        {
            //// Initialize the DocumentClient instance to be used to interact with the Azure Cosmos DB service
            //string accountName = ConfigurationManager.AppSettings["CosmosDBEndpointUri"];
            //string accountKey = ConfigurationManager.AppSettings["CosmosDBAuthKey"];
            //this.client = Utilities.CreateDocumentClient(accountName, accountKey);

            //// Create the test collection which will be monitored by the BackupExecutor for changes to be backed up to the specified Blob Storage account
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["DatabaseName"],
            //    ConfigurationManager.AppSettings["CollectionName"],
            //    true,
            //    "/partitionKey").Wait();

            //// Re-create the leases collection used by the ChangeFeedProcessor
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["leaseDbName"],
            //    ConfigurationManager.AppSettings["leaseCollectionName"],
            //    true,
            //    "/id").Wait();

            //// Re-eate the backup failure collection tracking failed backups to Blob Storage
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["BackupFailureDatabaseName"],
            //    ConfigurationManager.AppSettings["BackupFailureCollectionName"],
            //    true,
            //    "/id").Wait();

            //// Re-create the restore collection into which the backup will be restored
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["RestoreDatabaseName"],
            //    ConfigurationManager.AppSettings["RestoreCollectionName"],
            //    true,
            //    "/partitionKey").Wait();

            //// Re-reate the restore helper collection, which co-ordinates backups among the client machines to restore
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["RestoreHelperDatabaseName"],
            //    ConfigurationManager.AppSettings["RestoreHelperCollectionName"],
            //    true,
            //    "/id").Wait();

            //// Re-create the collection tracking successful restores
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["RestoreSuccessDatabaseName"],
            //    ConfigurationManager.AppSettings["RestoreSuccessCollectionName"],
            //    true,
            //    "/id").Wait();

            //// Re-reate the collection tracking blobs which were not successfully restored
            //Utilities.RecreateCollectionAsync(
            //    this.client,
            //    ConfigurationManager.AppSettings["RestoreFailureDatabaseName"],
            //    ConfigurationManager.AppSettings["RestoreFailureCollectionName"],
            //    true,
            //    "/id").Wait();

            //// Ensure the Blob Storage connection string can be parsed.
            //if (CloudStorageAccount.TryParse(ConfigurationManager.AppSettings["BlobStorageConnectionString"], out this.StorageAccount))
            //{
            //    this.CloudBlobClient = this.StorageAccount.CreateCloudBlobClient();
            //    List<string> containersInBlobStorageAccount = BlobStorageHelper.GetListOfContainersInStorageAccount(this.CloudBlobClient);

            //    foreach (string containerFromPreviousRun in containersInBlobStorageAccount)
            //    {
            //        CloudBlobContainer cloudBlobContainer = this.CloudBlobClient.GetContainerReference(containerFromPreviousRun);
            //        try
            //        {
            //            if (containerFromPreviousRun.StartsWith("backup"))
            //            {
            //                cloudBlobContainer.DeleteIfExists();
            //            }
            //        }
            //        catch (Exception ex)
            //        {
            //            Console.WriteLine(
            //                "Exception encountered when deleting Blob Storage containers from a previous test execution. Exceotion message: {0}",
            //                ex.Message);
            //        }
            //    }
            //}
            //else
            //{
            //    Console.WriteLine("The connection string for the Blob Storage Account is invalid. ");
            //    Console.ReadLine();
            //}
        }

        /// <summary>
        /// Validates the functioning of the BlobStorageExecutor by ensuring the documents inserted into the test collection
        /// were successfully backed up to the specified Blob Storage Account
        /// </summary>
        [TestMethod]
        public void TestRestoreToBlobStorageAccount()
        {
            //string hostName = string.Concat("Host-", Guid.NewGuid().ToString());

            //string restoreAccountName = ConfigurationManager.AppSettings["RestoreAccountUri"];
            //string restoreAccountKey = ConfigurationManager.AppSettings["RestoreAccountSecretKey"];
            //DocumentClient documentClientForRestore = Utilities.CreateDocumentClient(restoreAccountName, restoreAccountKey);

            //var tasks = new List<Task>();

            //// 1. Trigger the data generator to insert sample documents into the test collection
            //IDataGenerator dataGenerator = new EmployeeSampleDataGenerator(documentClientForRestore);
            //tasks.Add(Task.Factory.StartNew(() =>
            //{
            //    dataGenerator.GenerateSampleData().Wait();
            //}));

            //// 2. Trigger the backup executor to fetch all changes to the source collection and backup the changes to the specified Blob Storage Account
            //BackupExecutor backupExecutor = new BackupExecutor(client, hostName);
            //backupExecutor.ExecuteBackup().Wait();

            //// 3. Wait for both (1) DataGenerator and (2) BackupExecutor to finish execution
            //Task.WaitAll(tasks.ToArray());
            //Thread.Sleep(60 * 2 * 1000);

            //// 4. Validate the number of documents backed up to Blob Storage
            //int numberOfSampleDocumentsGenerated = int.Parse(ConfigurationManager.AppSettings["NumDocumentsToGenerate"]);
            //Assert.AreEqual(BlobStorageHelper.GetListOfDocumentsBackedUpInContainer(this.CloudBlobClient), numberOfSampleDocumentsGenerated);

            //tasks = new List<Task>();

            //RestoreExecutor restoreExecutor = new RestoreExecutor(client, hostName);
            //tasks.Add(Task.Factory.StartNew(() =>
            //{
            //    restoreExecutor.ExecuteRestore().Wait();
            //}));

            //Task.WaitAll(tasks.ToArray());

            //string monitoredDatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            //string monitoredCollectionName = ConfigurationManager.AppSettings["CollectionName"];

            //string restoreDatabaseName = ConfigurationManager.AppSettings["RestoreDatabaseName"];
            //string restoreCollectionName = ConfigurationManager.AppSettings["RestoreCollectionName"];

            //long documentCountInSourceCollection = CosmosDBHelper.FetchDocumentCountInCollection(this.client, monitoredDatabaseName, monitoredCollectionName).Result;
            //long documentCountInRestoredCollection = CosmosDBHelper.FetchDocumentCountInCollection(documentClientForRestore, restoreDatabaseName, restoreCollectionName).Result;

            //Assert.AreEqual(documentCountInSourceCollection, documentCountInRestoredCollection);
        }
    }
}
