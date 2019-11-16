namespace Microsoft.CosmosDB.PITRWithRestore
{
    using System;
    using System.Configuration;
    using System.Threading;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.CosmosDB.PITRWithRestore.Backup;
    using Microsoft.CosmosDB.PITRWithRestore.Restore;
    using Microsoft.CosmosDB.PITRWithRestore.CosmosDB;

    public class Program
    {
        public static void Main(string[] args)
        {
            string hostName = string.Concat("Newest-Host-", Guid.NewGuid().ToString());
            if (string.IsNullOrEmpty(ConfigurationManager.AppSettings["ModeOfOperation"]))
            {
                Console.WriteLine("Mode of operation [Backup/Restore] must be specified in configuration file. Please retry after setting mode of operation.");
            }
            else if (ConfigurationManager.AppSettings["ModeOfOperation"].Equals("Backup"))
            {
                string monitoredUri = ConfigurationManager.AppSettings["CosmosDBEndpointUri"];
                string monitoredSecretKey = ConfigurationManager.AppSettings["CosmosDBAuthKey"];

                DocumentClient client = CreateDocumentClient(monitoredUri, monitoredSecretKey);

                BackupExecutor backupExecutor = new BackupExecutor(client, hostName);
                backupExecutor.ExecuteBackup().Wait();
            }
            else if (ConfigurationManager.AppSettings["ModeOfOperation"].Equals("Restore"))
            {
                string restoreAccountUri = ConfigurationManager.AppSettings["RestoreAccountUri"];
                string restoreAccountSecretKey = ConfigurationManager.AppSettings["RestoreAccountSecretKey"];

                DocumentClient client = CreateDocumentClient(restoreAccountUri, restoreAccountSecretKey);

                RestoreExecutor restoreExecutor = new RestoreExecutor(client, hostName);
                restoreExecutor.ExecuteRestore().Wait();
            }

            Console.WriteLine("Completed!");
            Console.ReadLine();
        }

        /// <summary>
        /// Creates an instance of the DocumentClient to interact with Azure Cosmos DB Service
        /// </summary>
        /// <param name="accountName"></param>
        /// <param name="accountKey"></param>
        /// <returns></returns>
        private static DocumentClient CreateDocumentClient(string accountName, string accountKey)
        {
            DocumentClient client = new DocumentClient(new Uri(accountName), accountKey);
            return client;
        }
    }
}
