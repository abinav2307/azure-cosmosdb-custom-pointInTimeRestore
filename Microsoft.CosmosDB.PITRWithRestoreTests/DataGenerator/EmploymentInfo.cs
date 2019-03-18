
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    internal sealed class EmploymentInfo
    {
        /// <summary>
        /// List of Employers for this Employee
        /// </summary>
        [JsonProperty(PropertyName = "employers")]
        public List<Employer> Employers { get; set; }

        /// <summary>
        /// List of Positions held by this Employee
        /// </summary>
        [JsonProperty(PropertyName = "positions")]
        public List<Position> Positions { get; set; }
    }
}
