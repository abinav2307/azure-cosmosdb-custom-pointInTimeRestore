
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using Newtonsoft.Json;

    internal sealed class RestoreContainerDocument
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
        /// Name of this host machine performing the restore operation
        /// </summary>
        [JsonProperty(PropertyName = "restoreHostName")]
        public string RestoreHostName { get; set; }

        /// <summary>
        /// DocumentType specifying if the document is a Restore Success, Failure or Helper document
        /// </summary>
        [JsonProperty(PropertyName = "documentType")]
        public string DocumentType { get; set; }
    }
}
