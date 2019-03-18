
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    internal sealed class Employer
    {
        /// <summary>
        /// Name of the Employer
        /// </summary>
        [JsonProperty(PropertyName = "employerName")]
        public string EmployerName { get; set; }

        /// <summary>
        /// Number of positions held when employed by this Employer
        /// </summary>
        [JsonProperty(PropertyName = "positionsHeld")]
        public int PositionsHeld { get; set; }

        /// <summary>
        /// Number of managers when employed by this Employer
        /// </summary>
        [JsonProperty(PropertyName = "numManagers")]
        public int NumManagers { get; set; }

        /// <summary>
        /// Number of direct reports when employed by this Employer
        /// </summary>
        [JsonProperty(PropertyName = "numDirectReports")]
        public int NumDirectReports { get; set; }

        /// <summary>
        /// Name of a specific Product team when employed by this Employer
        /// </summary>
        [JsonProperty(PropertyName = "productName")]
        public string ProductName { get; set; }

        /// <summary>
        /// List of notable positiions held when employed by this Employer
        /// </summary>
        [JsonProperty(PropertyName = "notablePositions")]
        public List<NotablePosition> positions { get; set; }
    }
}
