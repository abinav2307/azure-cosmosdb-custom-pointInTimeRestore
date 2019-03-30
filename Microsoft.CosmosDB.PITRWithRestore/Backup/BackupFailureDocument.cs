using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace Microsoft.CosmosDB.PITRWithRestore.Backup
{
    internal sealed class BackupFailureDocument
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
        /// Compressed byte array which could not be backed up to the specified blob storage account
        /// </summary>
        [JsonProperty(PropertyName = "compressedByteArray")]
        public string CompressedByteArray { get; set; }

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
        [JsonProperty(PropertyName = "toString")]
        public string ToString { get; set; }
    }
}
