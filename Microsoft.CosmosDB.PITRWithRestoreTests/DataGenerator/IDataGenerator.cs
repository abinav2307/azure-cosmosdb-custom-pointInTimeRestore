
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System.Threading.Tasks;

    interface IDataGenerator
    {
        /// <summary>
        /// Generates sample data to be used by the tool. This could be:
        /// 1. Sample data over which aggregations need to be executed
        /// 2. Aggregation Rules which define the details of the aggregations to be executed
        /// </summary>
        /// <returns></returns>
        Task GenerateSampleData();
    }
}
