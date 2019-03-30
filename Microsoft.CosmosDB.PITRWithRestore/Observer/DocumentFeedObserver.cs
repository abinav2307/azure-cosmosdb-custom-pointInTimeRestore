
namespace Microsoft.CosmosDB.PITRWithRestore
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text;
    using System.IO;
    using System.IO.Compression;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.WindowsAzure.Storage.Blob;
    using Microsoft.CosmosDB.PITRWithRestore.BlobStorage;
    using Microsoft.CosmosDB.PITRWithRestore.CosmosDB;
    using Microsoft.CosmosDB.PITRWithRestore.Backup;

    using Newtonsoft.Json.Linq;

    /// <summary>
    /// This class implements the IChangeFeedObserver interface and is used to observe
    /// changes from the change feed. ChangeFeedEventHost will create as many instances of
    /// this class as needed.
    /// </summary>
    public class DocumentFeedObserver : IChangeFeedObserver
    {
        /// <summary>
        /// CloudBlobClient instance to push backups to the specified Azure Blob Storage Account
        /// </summary>
        private CloudBlobClient CloudBlobClient;

        /// <summary>
        /// Instance of the DocumentClient, used to push failed batches of backups to the Cosmos DB collection
        /// tracking failures during the backup process.
        /// </summary>
        private DocumentClient DocumentClient;

        /// <summary>
        /// Max number of retries on rate limited writes to the specified Blob Storage account
        /// </summary>
        private int MaxRetriesOnRateLimitedWritesToBlobAccount = 10;

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentFeedObserver" /> class.
        /// </summary>
        public DocumentFeedObserver(CloudBlobClient cloudBlobClient, DocumentClient client)
        {
            this.CloudBlobClient = cloudBlobClient;
            this.DocumentClient = client;
        }

        /// <summary>
        /// Called when change feed observer is opened; this function prints out the
        /// observer's partition key id.
        /// </summary>
        /// <param name="context">The context specifying the partition for this observer, etc.</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called when change feed observer is closed; this function prints out the
        /// observer's partition key id and reason for shut down.
        /// </summary>
        /// <param name="context">The context specifying the partition for this observer, etc.</param>
        /// <param name="reason">Specifies the reason the observer is closed.</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Called when there is a batch of changes to be processed.
        /// </summary>
        /// <param name="context">The context specifying the partition for this observer, etc.</param>
        /// <param name="docs">The documents changed.</param>
        /// <param name="cancellationToken">Token to signal that the parition processing is going to finish.</param>
        /// <returns>A Task to allow asynchronous execution</returns>
        public Task ProcessChangesAsync(IChangeFeedObserverContext context, IReadOnlyList<Document> docs, CancellationToken cancellationToken)
        {
            // Get Gzip compressed JArray, storing all documents returned from ChangeFeed for this PartitionKeyRangeId
            this.CompressDocumentsAndWriteToBlob(context.PartitionKeyRangeId, docs);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Fetches the Blob container from the Storage account and create it if it is unavailable.
        /// </summary>
        /// <param name="partitionId">The partition key range id to generate the blob container name</param>
        /// <param name="fileName">The file name, which is just the timestamp at which the data is being written with the doc count appended to it</param>
        /// <param name="compressedByteArray">The compressed byte array, which is a JArray of at most 100 documents, with the Json string Gzip compressed for 
        /// efficient storage into Blob Storage</param>
        private void WriteCompressedDataToBlob(string partitionId, string fileName, IReadOnlyList<Document> docs, byte[] compressedByteArray)
        {
            Console.WriteLine("Retrieved {0} documents from ChangeFeed", docs.Count);

            string containerName = string.Concat("backup-", partitionId);

            CloudBlobContainer cloudBlobContainer = this.CloudBlobClient.GetContainerReference(containerName);

            cloudBlobContainer.CreateIfNotExists();

            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);
            if(blockBlob.Exists())
            {
                if (!VerifyIfBlobPreviouslyBackedUp(blockBlob, compressedByteArray))
                {
                    try
                    {
                        fileName = string.Concat(fileName, "-", Guid.NewGuid());
                        blockBlob = cloudBlobContainer.GetBlockBlobReference(fileName);

                        BlobStorageHelper.WriteToBlobStorage(blockBlob, compressedByteArray, this.MaxRetriesOnRateLimitedWritesToBlobAccount);
                        this.TrackSuccessfulBatchesOfBackups(containerName, docs);
                    }
                    catch (Exception ex)
                    {
                        this.TrackFailedBatchesOfBackups(containerName, fileName, compressedByteArray, ex);
                    }
                }
                else
                {
                    this.TrackFailedBatchesOfBackups(containerName, fileName, compressedByteArray, null);
                }
            }
            else
            {
                try
                {
                    BlobStorageHelper.WriteToBlobStorage(blockBlob, compressedByteArray, this.MaxRetriesOnRateLimitedWritesToBlobAccount);
                    this.TrackSuccessfulBatchesOfBackups(containerName, docs);
                }
                catch (Exception ex)
                {
                    this.TrackFailedBatchesOfBackups(containerName, fileName, compressedByteArray, ex);
                }
            }
        }

        /// <summary>
        /// Verify if this blob was previously backed up
        /// </summary>
        /// <param name="blockBlob">CloudBlockBlob instance</param>
        /// <param name="compressedByteArray">Compressed byte array containing the list of documents in GZip compressed form</param>
        /// <returns></returns>
        private bool VerifyIfBlobPreviouslyBackedUp(CloudBlockBlob blockBlob, byte[] compressedByteArray)
        {
            bool isPreviouslyBackedUp = false;

            blockBlob.FetchAttributes();

            byte[] byteArray = new byte[blockBlob.Properties.Length];
            blockBlob.DownloadToByteArray(byteArray, 0);

            isPreviouslyBackedUp = StructuralComparisons.StructuralEqualityComparer.Equals(byteArray, compressedByteArray);
            return isPreviouslyBackedUp;
        }

        /// <summary>
        /// Keep track of the number of successfully backed up documents per container
        /// </summary>
        /// <param name="containerName">Name of the Blob Storage container containing the backups</param>
        /// <param name="docs">List of documents successfully backed up to the specified Blob Storage account</param>
        private void TrackSuccessfulBatchesOfBackups(string containerName, IReadOnlyList<Document> docs)
        {
            Console.WriteLine("Tracking successful backups to Cosmos DB collection tracking successful backups");

            string backupSuccessDatabaseName = ConfigurationManager.AppSettings["BackupSuccessDatabaseName"];
            string backupSuccessCollectionName = ConfigurationManager.AppSettings["BackupSuccessCollectionName"];
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(backupSuccessDatabaseName, backupSuccessCollectionName);

            BackupSuccessDocument backupSuccessDocument = new BackupSuccessDocument();
            backupSuccessDocument.ContainerName = containerName;
            backupSuccessDocument.Id = containerName;
            
            ResourceResponse<Document> document = CosmosDBHelper.ReadDocmentAsync(
                this.DocumentClient, 
                backupSuccessDatabaseName,
                backupSuccessCollectionName, 
                containerName, 
                containerName,
                this.MaxRetriesOnRateLimitedWritesToBlobAccount).Result;

            if(document == null)
            {
                backupSuccessDocument.DocumentCount = docs.Count;

                document = CosmosDBHelper.CreateDocumentAsync(
                    this.DocumentClient, 
                    backupSuccessDatabaseName, 
                    backupSuccessCollectionName, 
                    backupSuccessDocument, 
                    this.MaxRetriesOnRateLimitedWritesToBlobAccount).Result;
            }
            else
            {
                int numAttemptsIfConflict = 0;
                bool success = false;

                while (!success && numAttemptsIfConflict < this.MaxRetriesOnRateLimitedWritesToBlobAccount)
                {
                    int currentSuccessCount = document.Resource.GetPropertyValue<int>("documentCount");
                    backupSuccessDocument.DocumentCount = currentSuccessCount + docs.Count;

                    var ac = new AccessCondition { Condition = document.Resource.ETag, Type = AccessConditionType.IfMatch };
                    RequestOptions requestOptions = new RequestOptions { AccessCondition = ac };

                    document = CosmosDBHelper.ReplaceDocumentAsync(
                        this.DocumentClient,
                        backupSuccessDatabaseName,
                        backupSuccessCollectionName,
                        containerName,
                        backupSuccessDocument,
                        requestOptions,
                        this.MaxRetriesOnRateLimitedWritesToBlobAccount).Result;

                    numAttemptsIfConflict++;

                    if (document == null)
                    {
                        document = CosmosDBHelper.ReadDocmentAsync(
                            this.DocumentClient,
                            backupSuccessDatabaseName,
                            backupSuccessCollectionName,
                            containerName,
                            containerName,
                            this.MaxRetriesOnRateLimitedWritesToBlobAccount).Result;                        
                    }
                    else
                    {
                        success = true;
                    }
                }                
            }
        }

        /// <summary>
        /// Tracks failed batches of backups by writing the documents back to a smaller collection,
        /// solely responsible for storing documents, which could not be backed up to the Blob Storage Account.
        /// </summary>
        /// <param name="containerName">Container within which this backup should have been persisted</param>
        /// <param name="docs">Batch of created/updated documents from ChangeFeedProcessor which could not be backed up to the Azure Blob Storage Account</param>
        /// <param name="fileName">Name of the blob file which was not successfully persisted</param>
        /// <param name="ex">Exception which caused this backup to not be persisted in the specified blob storage account</param>
        private void TrackFailedBatchesOfBackups(string containerName, string fileName, byte[] docs, Exception ex)
        {
            Console.WriteLine("Tracking failed backups to Cosmos DB collecion tracking failures during backup process");

            string backupFailureDatabaseName = ConfigurationManager.AppSettings["BackupFailureDatabaseName"];
            string backupFailureCollectionName = ConfigurationManager.AppSettings["BackupFailureCollectionName"];
            string convertedByteArray = Encoding.UTF8.GetString(docs, 0, docs.Length);

            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(backupFailureDatabaseName, backupFailureCollectionName);

            BackupFailureDocument backupFailureDocument = new BackupFailureDocument();
            backupFailureDocument.ContainerName = containerName;
            backupFailureDocument.Id = fileName;
            backupFailureDocument.CompressedByteArray = convertedByteArray;

            if (ex != null)
            {
                backupFailureDocument.ExceptionType = ex.GetType().ToString();
                backupFailureDocument.ExceptionMessage = ex.Message;
                if (ex.InnerException != null)
                {
                    backupFailureDocument.InnerExceptionMessage = ex.InnerException.Message;
                }

                backupFailureDocument.ToString = ex.ToString();
            }
            else
            {
                backupFailureDocument.ToString = "Blob was previously backed up successfully. Ignoring!";
            }

            CosmosDBHelper.CreateDocumentAsync(
                this.DocumentClient, 
                backupFailureDatabaseName, 
                backupFailureCollectionName, 
                backupFailureDocument, 
                this.MaxRetriesOnRateLimitedWritesToBlobAccount).Wait();
        }

        /// <summary>
        /// Takes an input list of documents which have been returned by ChangeFeed for processing, and returns a GZip compressed byte array
        /// </summary>
        /// <param name="docs">List of documents which have been returned by ChangeFeed for processing</param>
        /// <returns></returns>
        private void CompressDocumentsAndWriteToBlob(string partitionKeyRangeId, IReadOnlyList<Document> docs)
        {
            // 1. Store the documents as a JArray and keep track of the max timestamp across all documents
            DateTime maxTimeStamp = DateTime.MinValue;
            DateTime minTimeStamp = DateTime.MaxValue;
            JArray jArrayOfChangedDocs = new JArray();
            foreach (Document doc in docs)
            {
                JObject eachDocumentAsJObject = JObject.Parse(doc.ToString());
                jArrayOfChangedDocs.Add(eachDocumentAsJObject);
                
                if (DateTime.Compare(maxTimeStamp, doc.Timestamp) < 0)
                {
                    maxTimeStamp = doc.Timestamp;
                }
                if (DateTime.Compare(minTimeStamp, doc.Timestamp) > 0)
                {
                    minTimeStamp = doc.Timestamp;
                }
            }

            // 2. Serialize the JArray and compress its contents (GZip compression)
            string jArraySerialized = jArrayOfChangedDocs.ToString();

            byte[] bytes = Encoding.ASCII.GetBytes(jArraySerialized);
            MemoryStream ms = new MemoryStream();
            GZipStream sw = new GZipStream(ms, CompressionMode.Compress);

            sw.Write(bytes, 0, bytes.Length);
            sw.Close();

            bytes = ms.ToArray();

            ms.Close();
            sw.Dispose();
            ms.Dispose();

            // 3. Set the filename for the destination file in Blob Storage to be a combination of the min and max timestamps
            // across all documents returned by ChangeFeedProcessor
            string fileName = string.Concat(
                minTimeStamp.ToString("yyyyMMddTHH:mm:ss"), 
                "-", 
                maxTimeStamp.ToString("yyyyMMddTHH:mm:ss"),
                "-",
                docs.Count);

            // 4. Write the compressed data to blob storage
            // Container name is the Partition's Id
            // Filename is the timestamp at which the data should be written
            this.WriteCompressedDataToBlob(partitionKeyRangeId, fileName, docs, bytes);

            Console.WriteLine("Successfully write compressed file to blob account with name: {0}", fileName);
        }    
    }
}
