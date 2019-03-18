
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using Newtonsoft.Json;

    internal sealed class Employee
    {
        /// <summary>
        /// Partition Key field for this entity when ingested into Cosmos DB
        /// </summary>
        [JsonProperty(PropertyName = "partitionKey")]
        public string PartitionKey { get; set; }

        /// <summary>
        /// First Name of this Employee
        /// </summary>
        [JsonProperty(PropertyName = "firstName")]
        public string FirstName { get; set; }

        /// <summary>
        /// Last Name of this Employee
        /// </summary>
        [JsonProperty(PropertyName = "lastName")]
        public string LastName { get; set; }

        /// <summary>
        /// Age of this Employee
        /// </summary>
        [JsonProperty(PropertyName = "age")]
        public int Age { get; set; }

        /// <summary>
        /// Title of this Employee
        /// </summary>
        [JsonProperty(PropertyName = "title")]
        public string Title { get; set; }

        /// <summary>
        /// Start Date of employment for this Employee
        /// </summary>
        [JsonProperty(PropertyName = "employmentStartDate")]
        public DateTime EmploymentStartDate { get; set; }

        /// <summary>
        /// Boolean flag indicating if this Employee is still employed
        /// </summary>
        [JsonProperty(PropertyName = "stillEmployed")]
        public bool StillEmployed { get; set; }

        /// <summary>
        /// Stock Symbol for this Employee's current Employer
        /// </summary>
        [JsonProperty(PropertyName = "currentEmployerTicker")]
        public string CurrentEmployerTicker { get; set; }

        /// <summary>
        /// Stock Symbol for this Employee's current Employer
        /// </summary>
        [JsonProperty(PropertyName = "employmentInfo")]
        public EmploymentInfo EmploymentInfo { get; set; }
    }
}
