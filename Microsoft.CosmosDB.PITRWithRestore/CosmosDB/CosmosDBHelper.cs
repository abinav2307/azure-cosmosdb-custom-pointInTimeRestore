
namespace Microsoft.CosmosDB.PITRWithRestore.CosmosDB
{
    using System;
    using System.Collections.ObjectModel;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents;

    using Microsoft.CosmosDB.PITRWithRestore.Logger;

    public class CosmosDBHelper
    {
        /// <summary>
        /// Writes a list of documents to the specified Cosmos DB database/collection
        /// </summary>
        /// <param name="client">DocumentClient instance used to interact with the Azure Cosmos DB service</param>
        /// <param name="databaseName">Database to write data to</param>
        /// <param name="collectionName">Collection to write data to</param>
        /// <param name="documentsToIngest">List of documents to ingest into the specified Cosmos DB collection</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Max retries on throttled requests to Cosmos DB</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> WriteDocumentsToCosmosDB(
            DocumentClient client,
            string databaseName,
            string collectionName,
            IEnumerable<Document> documentsToIngest,
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger)
        {
            int numRetries = 0;
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);
            ResourceResponse<Document> document = null;

            foreach (Document eachDocumentToIngest in documentsToIngest)
            {
                try
                {
                    document = await client.CreateDocumentAsync(documentsFeedLink, eachDocumentToIngest);
                }
                catch (DocumentClientException ex)
                {
                    if ((int)ex.StatusCode == 429)
                    {
                        logger.WriteMessage(string.Format("{0} - Received rate limiting exception when attempting to create document: {1}. Retrying", activityId, documentsFeedLink));

                        // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                        int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                        // Custom retry logic to keep retrying when the document read is rate limited
                        bool success = false;
                        while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                        {
                            // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                            Thread.Sleep(sleepTime);

                            try
                            {
                                document = await client.CreateDocumentAsync(documentsFeedLink, eachDocumentToIngest);
                            }
                            catch (DocumentClientException e)
                            {
                                if ((int)e.StatusCode == 429)
                                {
                                    logger.WriteMessage(string.Format("{0} - Still rate limited when attempting to read document: {1}. Retrying", activityId, documentsFeedLink));

                                    sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                    numRetries++;
                                }
                            }
                            catch (Exception exception)
                            {
                                logger.WriteMessage(string.Format("{0} - Caught Exception when retrying. Exception was: {1}. Will continue to retry.", activityId, exception.Message));
                                numRetries++;
                            }
                        }
                    }
                }
            }

            return document;
        }

        /// <summary>
        /// Reads a document from the specified Cosmos DB collection and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="partitionKey">Partition key of the document to read</param>
        /// <param name="id">Id property of the document to read</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> ReadDocmentAsync(
            DocumentClient client, 
            string databaseName, 
            string collectionName, 
            string partitionKey, 
            string id, 
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger)
        {
            int numRetries = 0;
            Uri documentsLink = UriFactory.CreateDocumentUri(databaseName, collectionName, id);
            ResourceResponse<Document> document = null;

            try
            {
                document = await client.ReadDocumentAsync(
                    documentsLink,
                    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
            }
            catch (DocumentClientException ex)
            {
                if((int)ex.StatusCode == 404)
                {
                    document = null;
                }
                else if ((int)ex.StatusCode == 429)
                {
                    logger.WriteMessage(string.Format("{0} - Received rate limiting exception when attempting to read document: {1}. Retrying", activityId, documentsLink));

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    // Custom retry logic to keep retrying when the document read is rate limited
                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            document = await client.ReadDocumentAsync(documentsLink);
                            logger.WriteMessage(string.Format("{0} - Document: {1}. not found", activityId, documentsLink));
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 404)
                            {
                                success = true;
                            }
                            else if ((int)e.StatusCode == 429)
                            {
                                logger.WriteMessage(string.Format("{0} - Still rate limited when attempting to read document: {1}. Retrying", activityId, documentsLink));
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                numRetries++;
                            }
                        }
                        catch (Exception exception)
                        {
                            logger.WriteMessage(string.Format("{0} - Caught Exception when retrying. Exception was: {1}. Will continue to retry.", activityId, exception.Message));
                            numRetries++;
                        }
                    }
                }
            }

            return document;
        }

        /// <summary>
        /// Deletes a document from the specified Cosmos DB collection and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="partitionKey">Partition key of the document to delete</param>
        /// <param name="id">Id property of the document to delete</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<bool> DeleteDocmentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            string partitionKey,
            string id,
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger)
        {
            int numRetries = 0;
            Uri documentsLink = UriFactory.CreateDocumentUri(databaseName, collectionName, id);
            ResourceResponse<Document> document = null;
            bool success = false;

            try
            {
                document = await client.DeleteDocumentAsync(
                    documentsLink,
                    new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });

                return true;
            }
            catch (DocumentClientException ex)
            {
                if ((int)ex.StatusCode == 404)
                {
                    success = true;
                }
                else if ((int)ex.StatusCode == 429)
                {
                    logger.WriteMessage(string.Format("{0} - Received rate limiting exception when attempting to delete document with id: {1}. Retrying", activityId, id));

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    // Custom retry logic to keep retrying when the document read is rate limited
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            await client.DeleteDocumentAsync(
                                documentsLink,
                                new RequestOptions { PartitionKey = new PartitionKey(partitionKey) });
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 404)
                            {
                                success = true;
                            }
                            else if ((int)e.StatusCode == 429)
                            {
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                                numRetries++;
                            }
                        }
                        catch (Exception exception)
                        {
                            logger.WriteMessage(string.Format("{0} - Caught Exception when retrying to delete document with id: {1}. Exception was: {2}", activityId, id, exception.Message));
                            numRetries++;
                        }
                    }
                }
            }

            return success;
        }

        /// <summary>
        /// Upserts the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to upsert</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<bool> UpsertDocumentAsync(
            DocumentClient client,
            string documentsFeedLink, 
            object document, 
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger,
            Exception exToLog = null,
            RequestOptions requestOptions = null)
        {
            int numRetries = 0;
            
            bool isUpsertSuccessful = false;
            try
            {
                await client.UpsertDocumentAsync(documentsFeedLink, document, null, true);
                isUpsertSuccessful = true;
            }
            catch (DocumentClientException ex)
            {
                exToLog = ex;

                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    logger.WriteMessage(string.Format("{0} - Received rate limiting exception when attempting to upsert document. Retrying", activityId));

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            await client.UpsertDocumentAsync(documentsFeedLink, document, null, true);
                            success = true;
                            isUpsertSuccessful = true;
                        }
                        catch (DocumentClientException e)
                        {
                            exToLog = e;

                            if ((int)e.StatusCode == 429)
                            {
                                logger.WriteMessage(string.Format("{0} - Still rate limited when attempting to upsert document. Retrying", activityId));
                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }
                            
                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            exToLog = exception;
                            logger.WriteMessage(string.Format("{0} - Caught Exception when retrying to upsert document. Exception was: {1}", activityId, exception.Message));
                            numRetries++;
                        }
                    }
                }
                else
                {
                    logger.WriteMessage(string.Format("{0} - Caught Exception when retrying to upsert document. Exception was: {1}. Status code was: {2}", activityId, ex.Message, (int)ex.StatusCode));
                }
            }
            catch (Exception e)
            {
                exToLog = e;
            }

            return isUpsertSuccessful;
        }

        /// <summary>
        /// Creates the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to create</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> ReplaceDocumentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            string documentId,
            object document,
            RequestOptions requestOptions,
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger)
        {
            int numRetries = 0;
            Uri documentUri = UriFactory.CreateDocumentUri(databaseName, collectionName, documentId);
            ResourceResponse<Document> replacedDocument = null;
            try
            {
                replacedDocument = await client.ReplaceDocumentAsync(documentUri.ToString(), document, requestOptions);
            }
            catch (DocumentClientException ex)
            {
                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    logger.WriteMessage(
                        string.Format(
                            "{0} - Received rate limiting exception when attempting to replace document with id: {1}. Retrying", 
                            activityId, 
                            documentId));

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            replacedDocument = await client.ReplaceDocumentAsync(documentUri.ToString(), document, requestOptions);
                            success = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 429)
                            {
                                logger.WriteMessage(
                                    string.Format(
                                        "{0} - Still rate limited when attempting to replace document with id: {1}. Retrying", 
                                        activityId, 
                                        documentId));

                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }

                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            logger.WriteMessage(
                                string.Format(
                                    "{0} - Caught Exception when retrying to replace document with id: {1}. Exception was: {2}", 
                                    activityId, 
                                    documentId, 
                                    exception.Message));

                            numRetries++;
                        }
                    }
                }
            }

            return replacedDocument;
        }

        /// <summary>
        /// Creates the specified document in Cosmos DB and retries when rate limited
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <param name="document">Document to create</param>
        /// <param name="maxRetriesOnDocumentClientExceptions">Maximum number of retries when rate limited</param>
        /// <returns></returns>
        public static async Task<ResourceResponse<Document>> CreateDocumentAsync(
            DocumentClient client,
            string databaseName,
            string collectionName,
            object document,
            int maxRetriesOnDocumentClientExceptions,
            string activityId,
            ILogger logger)
        {
            int numRetries = 0;
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName);

            ResourceResponse<Document> createdDocument = null;
            try
            {
                createdDocument = await client.CreateDocumentAsync(documentsFeedLink, document, null, true);
            }
            catch (DocumentClientException ex)
            {
                // Retry when rate limited for as many times as specified
                if ((int)ex.StatusCode == 429)
                {
                    logger.WriteMessage(
                        string.Format(
                            "{0} - Received rate limiting exception when attempting to create document. Retrying",
                            activityId));

                    // If the write is rate limited, wait for twice the recommended wait time specified in the exception
                    int sleepTime = (int)ex.RetryAfter.TotalMilliseconds * 2;

                    bool success = false;
                    while (!success && numRetries <= maxRetriesOnDocumentClientExceptions)
                    {
                        // Sleep for twice the recommended amount from the Cosmos DB rate limiting exception
                        Thread.Sleep(sleepTime);

                        try
                        {
                            createdDocument = await client.CreateDocumentAsync(documentsFeedLink, document, null, true);
                            success = true;
                        }
                        catch (DocumentClientException e)
                        {
                            if ((int)e.StatusCode == 429)
                            {
                                logger.WriteMessage(
                                    string.Format(
                                        "{0} - Still rate limited when attempting to create document. Retrying",
                                        activityId));

                                sleepTime = (int)e.RetryAfter.TotalMilliseconds * 2;
                            }

                            numRetries++;
                        }
                        catch (Exception exception)
                        {
                            logger.WriteMessage(
                                string.Format(
                                    "{0} - Caught Exception when retrying to create document. Exception was: {1}",
                                    activityId,
                                    exception.Message));

                            numRetries++;
                        }
                    }
                }
            }

            return createdDocument;
        }

        /// <summary>
        /// Returns a count of documents in the specified Cosmos DB collection
        /// </summary>
        /// <param name="client">DocumentClient instance to interact with Azure Cosmos DB Service</param>
        /// <param name="databaseName">Database name of the collection containing the document to read</param>
        /// <param name="collectionName">Collection name containing the document</param>
        /// <returns></returns>
        public static async Task<long> FetchDocumentCountInCollection(DocumentClient client, string databaseName, string collectionName)
        {
            string documentCollectionLink = UriFactory.CreateDocumentCollectionUri(databaseName, collectionName).ToString();

            ResourceResponse<DocumentCollection> resourceResponse =
                await client.ReadDocumentCollectionAsync(documentCollectionLink, new RequestOptions { PopulatePartitionKeyRangeStatistics = true });

            long documentCount = 0;
            foreach (PartitionKeyRangeStatistics eachPartitionsStats in resourceResponse.Resource.PartitionKeyRangeStatistics)
            {
                documentCount += eachPartitionsStats.DocumentCount;
            }

            return documentCount;
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
        /// <param name="databaseName">Name of database to create</param>
        /// <param name="collectionName">Name of collection to create within the specified database</param>
        /// <param name="throughput">Amount of throughput to provision for the collection to be created</param>
        /// <param name="partitionKey">Partition Key for the collection to be created</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        public static async Task CreateCollectionIfNotExistsAsync(
            DocumentClient client, 
            string databaseName, 
            string collectionName, 
            int throughput, 
            string partitionKey,
            ILogger logger,
            bool indexAllProperties = false,
            bool deleteExistingColl = false)
        {
            await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });

            PartitionKeyDefinition pkDefn = null;

            Collection<string> paths = new Collection<string>();
            paths.Add(partitionKey);
            pkDefn = new PartitionKeyDefinition() { Paths = paths };

            try
            {
                DocumentCollection existingColl = await client.ReadDocumentCollectionAsync(string.Format("/dbs/{0}/colls/{1}", databaseName, collectionName));
                if (existingColl != null)
                {
                    if (!deleteExistingColl)
                    {
                        logger.WriteMessage(string.Format("{0} - Collection already present. Continuing without delete...", collectionName));
                    }
                    else
                    {
                        await client.DeleteDocumentCollectionAsync(string.Format("/dbs/{0}/colls/{1}", databaseName, collectionName));
                        
                        IndexingPolicy policy = new IndexingPolicy();
                        if(!indexAllProperties)
                        {
                            policy.Automatic = false;
                            policy.IndexingMode = IndexingMode.None;
                            policy.IncludedPaths.Clear();
                            policy.ExcludedPaths.Clear();
                        }
                        await client.CreateDocumentCollectionAsync(
                            UriFactory.CreateDatabaseUri(databaseName),
                            new DocumentCollection { Id = collectionName, PartitionKey = pkDefn, IndexingPolicy = policy },
                            new RequestOptions { OfferThroughput = throughput });

                        logger.WriteMessage(string.Format("{0} - Collection was already present. Deleted and recreated the collection...", collectionName));
                    }
                }
            }
            catch (DocumentClientException dce)
            {
                if ((int)dce.StatusCode == 404)
                {
                    try
                    {
                        IndexingPolicy policy = new IndexingPolicy();
                        if (!indexAllProperties)
                        {
                            policy.Automatic = false;
                            policy.IndexingMode = IndexingMode.None;
                            policy.IncludedPaths.Clear();
                            policy.ExcludedPaths.Clear();
                        }

                        await client.CreateDocumentCollectionAsync(
                            UriFactory.CreateDatabaseUri(databaseName),
                            new DocumentCollection { Id = collectionName, PartitionKey = pkDefn, IndexingPolicy = policy },
                            new RequestOptions { OfferThroughput = throughput });

                        logger.WriteMessage(string.Format("{0} - Collection was not present. Successfully created the collection", collectionName));
                    }
                    catch (Exception ex)
                    {
                        logger.WriteMessage(string.Format(
                            "Exception thrown when attempting to create the collection {0} in database: {1}: with message: {2}", 
                            collectionName, 
                            databaseName, 
                            ex.Message));
                    }
                }
                else

                    throw;
            }
        }
    }
}
