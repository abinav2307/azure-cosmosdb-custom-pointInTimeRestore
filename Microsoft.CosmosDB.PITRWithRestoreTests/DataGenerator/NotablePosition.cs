
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    internal sealed class NotablePosition
    {
        /// <summary>
        /// Start date for this notable position with the employer
        /// </summary>
        [JsonProperty(PropertyName = "startDate")]
        public DateTime StartDate { get; set; }

        /// <summary>
        /// End date for this notable position with the employer
        /// </summary>
        [JsonProperty(PropertyName = "endDate")]
        public DateTime EndDate { get; set; }

        /// <summary>
        /// Position type of this position with the employer
        /// </summary>
        [JsonProperty(PropertyName = "positionType")]
        public PositionType PositionType { get; set; }
    }
}
