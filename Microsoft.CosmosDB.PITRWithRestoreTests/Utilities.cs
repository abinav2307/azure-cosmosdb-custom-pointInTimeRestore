
namespace Microsoft.CosmosDB.PITRWithRestoreTests
{
    using System;
    using System.Configuration;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    public class Utilities
    {
        public static async Task<DocumentCollection> RecreateCollectionAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            bool isPartitionedCollection,
            string partitionKey,
            string partitionKeyDefinition = null,
            bool isMongoCollection = false)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                await client.CreateDatabaseAsync(
                    new Database { Id = databaseName });
            }

            DocumentCollection collection = null;

            try
            {
                collection = await client.ReadDocumentCollectionAsync(
                        UriFactory.CreateDocumentCollectionUri(
                            databaseName,
                            collectionName))
                    .ConfigureAwait(false);
            }
            catch (DocumentClientException)
            {
            }

            if (collection != null)
            {
                await client.DeleteDocumentCollectionAsync(
                    UriFactory.CreateDocumentCollectionUri(
                        databaseName,
                        collectionName)).ConfigureAwait(false);
            }

            DocumentCollection myCollection = new DocumentCollection() { Id = collectionName };
            
            if (!string.IsNullOrWhiteSpace(partitionKey) && isPartitionedCollection)
            {
                myCollection.PartitionKey.Paths.Add(partitionKey);
            }

            myCollection.DefaultTimeToLive = -1;

            return await client.CreateDocumentCollectionAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                myCollection,
                new RequestOptions { OfferThroughput = int.Parse(ConfigurationManager.AppSettings["OfferThroughput"]) });
        }

        public static Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        public static async Task DeleteDatabase(DocumentClient client, String databaseName)
        {
            await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(databaseName));
        }

        /// <summary>
        /// Creates an instance of the DocumentClient to interact with the Azure Cosmos DB service
        /// </summary>
        /// <returns></returns>
        public static DocumentClient CreateDocumentClient(string accountName, string accountKey)
        {
            ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp
            };

            DocumentClient documentClient = null;
            try
            {
                documentClient = new DocumentClient(new Uri(accountName), accountKey, ConnectionPolicy);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception while creating DocumentClient. Original  exception message was: {0}", ex.Message);
            }

            return documentClient;
        }
    }
}
