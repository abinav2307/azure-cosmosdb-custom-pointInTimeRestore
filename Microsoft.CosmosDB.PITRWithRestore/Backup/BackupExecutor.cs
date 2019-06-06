
namespace Microsoft.CosmosDB.PITRWithRestore.Backup
{
    using System;
    using System.Configuration;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.CosmosDB.PITRWithRestore.CosmosDB;
    using Microsoft.CosmosDB.PITRWithRestore.Logger;

    public class BackupExecutor
    {
        /// <summary>
        /// Instance of the DocumentClient, used to push failed batches of backups to the Cosmos DB collection
        /// tracking failures during the backup process.
        /// </summary>
        private DocumentClient DocumentClient;

        /// <summary>
        /// Name of this host running the Backup job
        /// </summary>
        private string HostName;

        /// <summary>
        /// Logger to push messages during run time
        /// </summary>
        private ILogger Logger;

        public BackupExecutor(DocumentClient client, string hostName)
        {
            this.DocumentClient = client;
            this.HostName = hostName;
            this.Logger = new LogAnalyticsLogger();
        }

        /// <summary>
        /// Main Async function; checks for or creates monitored/lease
        /// collections and runs Change Feed Host
        /// (<see cref="RunChangeFeedHostAsync" />)
        /// </summary>
        /// <returns>A Task to allow asynchronous execution</returns>
        public async Task ExecuteBackup()
        {
            string leaseDbName = ConfigurationManager.AppSettings["leaseDbName"];
            string leaseCollectionName = ConfigurationManager.AppSettings["leaseCollectionName"];
            int leaseThroughput = int.Parse(ConfigurationManager.AppSettings["leaseThroughput"]);
            string leaseCollectionPartitionKey = ConfigurationManager.AppSettings["leaseCollectionPartitionKey"];

            await CosmosDBHelper.CreateCollectionIfNotExistsAsync(this.DocumentClient, leaseDbName, leaseCollectionName, leaseThroughput, leaseCollectionPartitionKey, this.Logger);

            await this.RunChangeFeedHostAsync();
        }

        /// <summary>
        /// Registers a change feed observer to update changes read on
        /// change feed to destination collection. Deregisters change feed
        /// observer and closes process when enter key is pressed
        /// </summary>
        /// <returns>A Task to allow asynchronous execution</returns>
        private async Task RunChangeFeedHostAsync()
        {
            string monitoredUri = ConfigurationManager.AppSettings["CosmosDBEndpointUri"];
            string monitoredSecretKey = ConfigurationManager.AppSettings["CosmosDBAuthKey"];
            string monitoredDbName = ConfigurationManager.AppSettings["DatabaseName"];
            string monitoredCollectionName = ConfigurationManager.AppSettings["CollectionName"];
            
            // Source collection to be monitored for changes
            DocumentCollectionInfo documentCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(monitoredUri),
                MasterKey = monitoredSecretKey,
                DatabaseName = monitoredDbName,
                CollectionName = monitoredCollectionName
            };

            string leaseUri = ConfigurationManager.AppSettings["leaseUri"];
            string leaseSecretKey = ConfigurationManager.AppSettings["leaseSecretKey"];
            string leaseDbName = ConfigurationManager.AppSettings["leaseDbName"];
            string leaseCollectionName = ConfigurationManager.AppSettings["leaseCollectionName"];
            
            // Lease Collection managing leases on each of the underlying shards of the source collection being monitored
            DocumentCollectionInfo leaseCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(leaseUri),
                MasterKey = leaseSecretKey,
                DatabaseName = leaseDbName,
                CollectionName = leaseCollectionName
            };

            DocumentFeedObserverFactory docObserverFactory = new DocumentFeedObserverFactory(this.DocumentClient);
            ChangeFeedProcessorOptions feedProcessorOptions = new ChangeFeedProcessorOptions();

            int feedPollDelayInSeconds = int.Parse(ConfigurationManager.AppSettings["FeedPollDelayInSeconds"]);
            feedProcessorOptions.LeaseRenewInterval = TimeSpan.FromSeconds(240);
            feedProcessorOptions.LeaseExpirationInterval = TimeSpan.FromSeconds(240);
            feedProcessorOptions.FeedPollDelay = TimeSpan.FromMilliseconds(feedPollDelayInSeconds * 1000);
            feedProcessorOptions.StartFromBeginning = true;
            feedProcessorOptions.MaxItemCount = 2000;

            ChangeFeedProcessorBuilder builder = new ChangeFeedProcessorBuilder();
            builder
                .WithHostName(this.HostName)
                .WithFeedCollection(documentCollectionInfo)
                .WithLeaseCollection(leaseCollectionInfo)
                .WithProcessorOptions(feedProcessorOptions)
                .WithObserverFactory(new DocumentFeedObserverFactory(this.DocumentClient));

            var result = await builder.BuildAsync();
            await result.StartAsync().ConfigureAwait(false);
        }
    }
}
