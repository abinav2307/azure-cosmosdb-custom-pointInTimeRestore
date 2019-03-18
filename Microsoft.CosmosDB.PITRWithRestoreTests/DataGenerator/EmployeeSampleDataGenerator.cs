
namespace Microsoft.CosmosDB.PITRWithRestoreTests.DataGenerator
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Configuration;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    internal sealed class EmployeeSampleDataGenerator : IDataGenerator
    {
        /// <summary>
        /// The average size of documents to generate and ingest into Cosmos DB
        /// </summary>
        public int DocSizeInKb { get; set; }

        /// <summary>
        /// Number of sample Employers to generate for each Employee
        /// </summary>
        private int NumEmployers;

        /// <summary>
        /// Number of sample Notable Positions to generate for each Employee
        /// </summary>
        private int NumNotablePositions;

        /// <summary>
        /// Number of sample Positions to generate for each Employee
        /// </summary>
        private int NumPositions;

        /// <summary>
        /// The database containing the collection, into which the sample data will be ingested
        /// </summary>
        private string DatabaseName;

        /// <summary>
        /// The Cosmos DB collection into which the sample data will be ingested
        /// </summary>
        private string CollectionName;

        /// <summary>
        /// DocumentClient instance to be used for the ingestion
        /// </summary>
        private DocumentClient DocumentClient;

        /// <summary>
        /// Random number generator to assign a random title or department to a User entity in the sample data
        /// </summary>
        private static Random random = new Random();

        /// <summary>
        /// Maximum number of custom retries on throttled requests
        /// </summary>
        private int MaxRetriesOnThrottles = 10;

        /// <summary>
        /// Creates a new instance of this DataGenerator
        /// </summary>
        /// <param name="DocumentClient">The Cosmos DB DocumentClient</param>
        public EmployeeSampleDataGenerator(DocumentClient client)
        {
            this.DocSizeInKb = int.Parse(ConfigurationManager.AppSettings["DocSizesInKb"]);
            this.DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
            this.CollectionName = ConfigurationManager.AppSettings["CollectionName"];
            
            try
            {
                this.DocumentClient = client;
            }
            catch (DocumentClientException ex)
            {
                Console.WriteLine("Exception thrown when initializing the DocumentClient instance");
            }

            DetermineDocumentParameters();
        }

        /// <summary>
        /// Generates sample data
        /// </summary>
        public async Task GenerateSampleData()
        {
            List<string> documentsToImport = new List<string>();
            int numEmployeesToGenerate = int.Parse(ConfigurationManager.AppSettings["NumDocumentsToGenerate"]);
            int batchSizeForWrites = int.Parse(ConfigurationManager.AppSettings["BatchSizeForWrites"]);

            int currentBatchCount = 0;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            int batchNum = 1;
            for (int eachEmployeeToGenerate = 0; eachEmployeeToGenerate < numEmployeesToGenerate; eachEmployeeToGenerate++)
            {
                Employee sampleEmployee = GenerateSampleEmployeeData();

                string employeeJson = JsonConvert.SerializeObject(sampleEmployee);
                documentsToImport.Add(employeeJson);
                currentBatchCount++;

                if (currentBatchCount == batchSizeForWrites)
                {
                    Console.WriteLine("Batch: {0}", batchNum);
                    batchNum++;

                    await this.WriteToCosmosDB(documentsToImport);
                    currentBatchCount = 0;
                    documentsToImport = new List<string>();
                }
            }

            // Flush the last batch of documents to write (if any)
            if(documentsToImport.Count > 0)
            {
                await this.WriteToCosmosDB(documentsToImport);
            }

            stopwatch.Stop();
            Console.WriteLine("Time taken = {0} milliseconds", stopwatch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Writes the randomly generated sample data into the specified Cosmos DB collection, using the BulkExecutor library
        /// </summary>
        /// <param name="documentsToImport"></param>
        private async Task WriteToCosmosDB(List<string> documentsToImport)
        {
            Uri documentsFeedLink = UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName);

            foreach (string eachDocumentToWrite in documentsToImport)
            {
                try
                {
                    await this.DocumentClient.UpsertDocumentAsync(
                        documentsFeedLink,
                        JObject.Parse(eachDocumentToWrite),
                        null,
                        false);

                    Console.WriteLine("Successfully wrote document to Cosmos DB collection");
                }
                catch (DocumentClientException ex)
                {
                    if ((int)ex.StatusCode == 429)
                    {
                        // Implement custom retry logic - retry another 10 times
                        int numRetries = 0;
                        bool success = false;

                        while (!success && numRetries < this.MaxRetriesOnThrottles)
                        {
                            Thread.Sleep((int)(ex.RetryAfter.TotalMilliseconds * 2));

                            try
                            {
                                await this.DocumentClient.UpsertDocumentAsync(documentsFeedLink, eachDocumentToWrite, null, false);
                                success = true;
                            }
                            catch (DocumentClientException idex)
                            {
                                numRetries++;
                            }
                            catch (Exception iex)
                            {
                                numRetries++;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception encountered when attempting to write document. Original exception was: {0}", ex.Message);
                }
            }
        }

        /// <summary>
        /// Determines the number of entities to be generate within each document, based on the
        /// specified size of each sample document
        /// </summary>
        private void DetermineDocumentParameters()
        {
            switch (this.DocSizeInKb)
            {
                case 2:
                    this.NumEmployers = 1;
                    this.NumNotablePositions = 5;
                    this.NumPositions = 4;
                    break;
                case 5:
                    this.NumEmployers = 4;
                    this.NumNotablePositions = 3;
                    this.NumPositions = 12;
                    break;
                case 1:
                default:
                    this.NumEmployers = 1;
                    this.NumNotablePositions = 1;
                    this.NumPositions = 1;
                    break;
            }
        }

        /// <summary>
        /// Generates a sample entity of the Employee class
        /// </summary>
        /// <returns></returns>
        private Employee GenerateSampleEmployeeData()
        {
            Employee employee = new Employee();
            employee.FirstName = Constants.GetRandomFirstName();
            employee.LastName = Constants.GetRandomLastName();
            employee.EmploymentStartDate = DateTime.Now.AddMonths(-72); ;
            employee.Age = random.Next(22, 50);
            employee.Title = Constants.GetRandomTitle();
            employee.StillEmployed = true;
            employee.CurrentEmployerTicker = "MSFT";

            employee.PartitionKey = string.Concat(employee.FirstName, " ", employee.LastName);

            employee.EmploymentInfo = new EmploymentInfo();
            employee.EmploymentInfo.Employers = GenerateSampleEmployerData(NumEmployers, NumNotablePositions);
            employee.EmploymentInfo.Positions = GenerateSamplePositions(NumPositions);

            return employee;
        }

        /// <summary>
        /// Generates a sample list of Employer entities
        /// </summary>
        /// <param name="numEmployers">Number of employers to include in the List of Employers</param>
        /// <param name="numNotablePositions">Number of notable positions to generate for the Employee</param>
        /// <returns></returns>
        private List<Employer> GenerateSampleEmployerData(int numEmployers, int numNotablePositions)
        {
            List<Employer> employers = new List<Employer>();

            DateTime earliestDate = DateTime.Now.AddMonths(-72);

            for (int eachEmployer = 0; eachEmployer < numEmployers; eachEmployer++)
            {
                Employer employer = new Employer();
                employer.EmployerName = "Microsoft Corporation";
                employer.PositionsHeld = random.Next(1, 10);
                employer.NumManagers = random.Next(1, employer.PositionsHeld);
                employer.NumDirectReports = random.Next(0, 5);
                employer.ProductName = Constants.GetRandomProductName();
                employer.positions = GenerateSampleNotablePositions(numNotablePositions);

                employers.Add(employer);
            }

            return employers;
        }

        /// <summary>
        /// Generates a list of Notable Positions for the Employee
        /// </summary>
        /// <param name="numNotablePositions">Number of Notable Employees to generate for this Employee</param>
        /// <returns></returns>
        private List<NotablePosition> GenerateSampleNotablePositions(int numNotablePositions)
        {
            List<NotablePosition> notablePositions = new List<NotablePosition>();

            DateTime earliestDate = DateTime.Now.AddMonths(-72);

            for (int eachNotablePosition = 0; eachNotablePosition < numNotablePositions; eachNotablePosition++)
            {
                NotablePosition notablePosition = new NotablePosition();
                notablePosition.StartDate = earliestDate;
                notablePosition.EndDate = earliestDate.AddMonths(1);
                notablePosition.PositionType = PositionType.FULL_TIME;

                notablePositions.Add(notablePosition);
            }

            return notablePositions;
        }

        /// <summary>
        /// Number of detailed positions to generate for this Employee
        /// </summary>
        /// <param name="numPositions">Number of positions to generate for this Employee</param>
        /// <returns></returns>
        private List<Position> GenerateSamplePositions(int numPositions)
        {
            List<Position> positions = new List<Position>();

            DateTime earliestDate = DateTime.Now.AddMonths(-72);

            for (int eachPosition = 0; eachPosition < numPositions; eachPosition++)
            {
                Position position = new Position();
                position.StartDate = earliestDate;
                position.EndDate = earliestDate.AddMonths(1);
                position.ManagedBy = String.Concat(Constants.GetRandomFirstName(), " ", Constants.GetRandomLastName());
                position.NumAwardedStockUnits = random.Next(0, 50);
                position.NumVestedStockUnits = random.Next(0, position.NumAwardedStockUnits);
                position.Salary = random.Next(100000, 500000);

                positions.Add(position);
            }

            return positions;
        }
    }
}
