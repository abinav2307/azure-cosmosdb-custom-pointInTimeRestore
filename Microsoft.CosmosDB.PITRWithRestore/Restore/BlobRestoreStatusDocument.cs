
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
        /// 
        /// </summary>
        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }

        /// <summary>
        /// Number of documents restored for the container
        /// </summary>
        [JsonProperty(PropertyName = "documentCount")]
        public long DocumentCount { get; set; }

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
