
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;

    internal sealed class BlobRestoreStatusDocument
    {
        /// <summary>
        /// Name of the Blob Storage container, with the backups to be restored
        /// </summary>
        [JsonProperty(PropertyName = "containerName")]
        public string ContainerName { get; set; }

        /// <summary>
        /// Name of this host machine performing the restore operation
        /// </summary>
        [JsonProperty(PropertyName = "restoreHostName")]
        public string RestoreHostName { get; set; }

        /// <summary>
        /// id of the document once it is persisted in Cosmos DB
        /// </summary>
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        /// <summary>
        /// Name of the blob file containing the Cosmos DB backups to be restored
        /// </summary>
        [JsonProperty(PropertyName = "blobName")]
        public string BlobName { get; set; }

        /// <summary>
        /// DocumentType specifying if the document is a Restore Success, Failure or Helper document
        /// </summary>
        [JsonProperty(PropertyName = "documentType")]
        public string DocumentType { get; set; }

        /// <summary>
        /// Status of the restore operation
        /// </summary>
        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }

        /// <summary>
        /// Number of backup blobs successfully restored for the container
        /// </summary>
        [JsonProperty(PropertyName = "successfullyRestoredBlobCount")]
        public long SuccessfullyRestoredBlobCount { get; set; }

        /// <summary>
        /// The exception message for the failure
        /// </summary>
        [JsonProperty(PropertyName = "exceptionMessage")]
        public string ExceptionMessage { get; set; }

        /// <summary>
        /// The exception type
        /// </summary>
        [JsonProperty(PropertyName = "exceptionType")]
        public string ExceptionType { get; set; }

        /// <summary>
        /// The exception type
        /// </summary>
        [JsonProperty(PropertyName = "innerExceptionMessage")]
        public string InnerExceptionMessage { get; set; }

        /// <summary>
        /// The exception type
        /// </summary>
        [JsonProperty(PropertyName = "documentFailedToRestore")]
        public JObject DocumentFailedToRestore { get; set; }
    }
}
