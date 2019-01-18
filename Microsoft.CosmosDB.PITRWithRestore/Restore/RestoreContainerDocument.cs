
namespace Microsoft.CosmosDB.PITRWithRestore.Restore
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Newtonsoft.Json;

    internal sealed class RestoreContainerDocument
    {
        [JsonProperty(PropertyName = "containerName")]
        public string ContainerName { get; set; }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "restoreHostName")]
        public string RestoreHostName { get; set; }
    }
}
