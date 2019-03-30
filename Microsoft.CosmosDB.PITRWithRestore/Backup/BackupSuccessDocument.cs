using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Newtonsoft.Json;

namespace Microsoft.CosmosDB.PITRWithRestore.Backup
{
    internal sealed class BackupSuccessDocument
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
        /// 
        /// </summary>
        [JsonProperty(PropertyName = "documentCount")]
        public int DocumentCount { get; set; }
    }
}
