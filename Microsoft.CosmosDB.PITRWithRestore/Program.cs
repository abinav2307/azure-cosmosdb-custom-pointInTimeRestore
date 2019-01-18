namespace Microsoft.CosmosDB.PITRWithRestore
{
    using System;
    using System.Linq;
    using System.Configuration;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.Logging;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.CosmosDB.PITRWithRestore.Backup;
    using Microsoft.CosmosDB.PITRWithRestore.Restore;
    using Newtonsoft.Json;

    public class Program
    {
        public static void Main(string[] args)
        {
            string hostName = string.Concat("Host-", Guid.NewGuid().ToString());

            if(string.IsNullOrEmpty(ConfigurationManager.AppSettings["ModeOfOperation"]))
            {
                Console.WriteLine("Mode of operation [Backup/Restore] must be specified in configuration file. Please retry after setting mode of operation.");                
            }
            else if (ConfigurationManager.AppSettings["ModeOfOperation"].Equals("Backup"))
            {
                string monitoredUri = ConfigurationManager.AppSettings["monitoredUri"];
                string monitoredSecretKey = ConfigurationManager.AppSettings["monitoredSecretKey"];

                DocumentClient client = new DocumentClient(new Uri(monitoredUri), monitoredSecretKey);

                BackupExecutor backupExecutor = new BackupExecutor(client, hostName);
                backupExecutor.ExecuteBackup().Wait();
            }
            else if (ConfigurationManager.AppSettings["ModeOfOperation"].Equals("Restore"))
            {
                string restoreAccountUri = ConfigurationManager.AppSettings["RestoreAccountUri"];
                string restoreAccountSecretKey = ConfigurationManager.AppSettings["RestoreAccountSecretKey"];

                DocumentClient client = new DocumentClient(new Uri(restoreAccountUri), restoreAccountSecretKey);

                RestoreExecutor restoreExecutor = new RestoreExecutor(client, hostName);
                restoreExecutor.ExecuteRestore().Wait();
            }

            Console.WriteLine("Completed!");
            Console.ReadLine();
        }        
    }
}
