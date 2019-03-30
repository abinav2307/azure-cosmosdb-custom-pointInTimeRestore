
namespace Microsoft.CosmosDB.PITRWithRestoreTests
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
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.CosmosDB.PITRWithRestore.BlobStorage;

    [TestClass]
    public class BackupTests
    {
        /// <summary>
        /// Instance of the DocumentClient, used to push failed batches of backups to the Cosmos DB collection
        /// tracking failures during the backup process.
        /// </summary>
        private DocumentClient client = null;

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
            // Initialize the DocumentClient instance to be used to interact with the Azure Cosmos DB service
            string accountName = ConfigurationManager.AppSettings["CosmosDBEndpointUri"];
            string accountKey = ConfigurationManager.AppSettings["CosmosDBAuthKey"];
            this.client = Utilities.CreateDocumentClient(accountName, accountKey);

            // Re-create the test collection which will be monitored by the BackupExecutor for changes to be backed up to the specified Blob Storage account
            Utilities.RecreateCollectionAsync(
                client,
                ConfigurationManager.AppSettings["DatabaseName"],
                ConfigurationManager.AppSettings["CollectionName"],
                true,
                "/partitionKey").Wait();

            // Re-create the leases collection used by the ChangeFeedProcessor
            Utilities.RecreateCollectionAsync(
                client,
                ConfigurationManager.AppSettings["leaseDbName"],
                ConfigurationManager.AppSettings["leaseCollectionName"],
                true,
                "/id").Wait();

            // Create the test collection which will be monitored by the BackupExecutor for changes to be backed up to the specified Blob Storage account
            Utilities.RecreateCollectionAsync(
                client,
                ConfigurationManager.AppSettings["BackupFailureDatabaseName"],
                ConfigurationManager.AppSettings["BackupFailureCollectionName"],
                true,
                "/id").Wait();

            CloudStorageAccount StorageAccount;

            // Ensure the Blob Storage connection string can be parsed.
            if (CloudStorageAccount.TryParse(ConfigurationManager.AppSettings["BlobStorageConnectionString"], out StorageAccount))
            {
                this.CloudBlobClient = StorageAccount.CreateCloudBlobClient();
                List<string> containersInBlobStorageAccount = BlobStorageHelper.GetListOfContainersInStorageAccount(this.CloudBlobClient);

                foreach (string containerFromPreviousRun in containersInBlobStorageAccount)
                {
                    CloudBlobContainer cloudBlobContainer = this.CloudBlobClient.GetContainerReference(containerFromPreviousRun);
                    try
                    {
                        if (containerFromPreviousRun.StartsWith("backup"))
                        {
                            cloudBlobContainer.DeleteIfExists();
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(
                            "Exception encountered when deleting Blob Storage containers from a previous test execution. Exceotion message: {0}",
                            ex.Message);
                    }
                }
            }
            else
            {
                Console.WriteLine("The connection string for the Blob Storage Account is invalid. ");
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Validates the functioning of the BackupExecutor by ensuring the documents inserted into the test collection
        /// were successfully backed up to the specified Blob Storage Account
        /// </summary>
        [TestMethod]
        public void TestBackupsToBlobStorageAccount()
        {
            string hostName = string.Concat("Host-", Guid.NewGuid().ToString());

            var tasks = new List<Task>();

            // 1. Trigger the data generator to insert sample documents into the test collection
            IDataGenerator dataGenerator = new EmployeeSampleDataGenerator(this.client);
            tasks.Add(Task.Factory.StartNew(() =>
            {
                dataGenerator.GenerateSampleData().Wait();
            }));

            // 2. Trigger the backup executor to fetch all changes to the source collection and backup the changes to the specified Blob Storage Account
            BackupExecutor backupExecutor = new BackupExecutor(client, hostName);
            backupExecutor.ExecuteBackup().Wait();

            // 3. Wait for both (1) DataGenerator and (2) BackupExecutor to finish execution
            Task.WaitAll(tasks.ToArray());
            Thread.Sleep(60 * 2 * 1000);

            //// 4. Validate the number of documents backed up to Blob Storage
            int numberOfSampleDocumentsGenerated = int.Parse(ConfigurationManager.AppSettings["NumDocumentsToGenerate"]);
            Console.WriteLine("Count retrieved = {0}", BlobStorageHelper.GetListOfDocumentsBackedUpInContainer(this.CloudBlobClient));
            Assert.AreEqual(BlobStorageHelper.GetListOfDocumentsBackedUpInContainer(this.CloudBlobClient), numberOfSampleDocumentsGenerated);
        }
    }
}
