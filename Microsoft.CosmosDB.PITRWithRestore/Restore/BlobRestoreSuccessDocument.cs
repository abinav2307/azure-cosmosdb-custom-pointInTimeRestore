
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    internal sealed class BlobRestoreSuccessDocument
    {
        [JsonProperty(PropertyName = "containerName")]
        public string ContainerName { get; set; }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "blobName")]
        public string BlobName { get; set; }

        [JsonProperty(PropertyName = "status")]
        public string Status { get; set; }
    }
}
