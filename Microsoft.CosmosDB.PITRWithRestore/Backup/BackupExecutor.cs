
namespace Microsoft.CosmosDB.PITRWithRestore.Backup
{
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents;

    internal sealed class BackupExecutor
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

        public BackupExecutor(DocumentClient client, string hostName)
        {
            this.DocumentClient = client;
            this.HostName = hostName;
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

            await this.CreateCollectionIfNotExistsAsync(
                leaseDbName,
                leaseCollectionName,
                leaseThroughput);

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
            string monitoredUri = ConfigurationManager.AppSettings["monitoredUri"];
            string monitoredSecretKey = ConfigurationManager.AppSettings["monitoredSecretKey"];
            string monitoredDbName = ConfigurationManager.AppSettings["monitoredDbName"];
            string monitoredCollectionName = ConfigurationManager.AppSettings["monitoredCollectionName"];
            int monitoredThroughput = int.Parse(ConfigurationManager.AppSettings["monitoredThroughput"]);

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
            int leaseThroughput = int.Parse(ConfigurationManager.AppSettings["leaseThroughput"]);

            // Lease Collection managing leases on each of the underlying shards of the source collection
            DocumentCollectionInfo leaseCollectionInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(leaseUri),
                MasterKey = leaseSecretKey,
                DatabaseName = leaseDbName,
                CollectionName = leaseCollectionName
            };

            DocumentFeedObserverFactory docObserverFactory = new DocumentFeedObserverFactory(this.DocumentClient);
            ChangeFeedProcessorOptions feedProcessorOptions = new ChangeFeedProcessorOptions();

            feedProcessorOptions.LeaseRenewInterval = TimeSpan.FromSeconds(240);
            feedProcessorOptions.LeaseExpirationInterval = TimeSpan.FromSeconds(240);
            feedProcessorOptions.FeedPollDelay = TimeSpan.FromMilliseconds(1);
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

        /// <summary>
        /// Checks whether a collections exists. Creates a new collection if
        /// the collection does not exist.
        /// <para>WARNING: CreateCollectionIfNotExistsAsync will create a
        /// new collection with reserved throughput which has pricing
        /// implications. For details visit:
        /// https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
        /// </para>
        /// </summary>
        /// <param name="databaseName">Name of database </param>
        /// <param name="collectionName">Name of collection</param>
        /// <param name="throughput">Amount of throughput to provision</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        private async Task CreateCollectionIfNotExistsAsync(string databaseName, string collectionName, int throughput)
        {
            await this.DocumentClient.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });

            PartitionKeyDefinition pkDefn = null;

            Collection<string> paths = new Collection<string>();
            paths.Add("/id");
            pkDefn = new PartitionKeyDefinition() { Paths = paths };

            // create collection if it does not exist
            // WARNING: CreateDocumentCollectionIfNotExistsAsync will
            // create a new collection with reserved throughput which
            // has pricing implications. For details visit:
            // https://azure.microsoft.com/en-us/pricing/details/cosmos-db/
            await this.DocumentClient.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection { Id = collectionName },
                new RequestOptions { OfferThroughput = throughput });
        }
    }
}
