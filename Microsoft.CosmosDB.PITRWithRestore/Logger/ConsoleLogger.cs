using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.CosmosDB.PITRWithRestore.Logger
{
    class ConsoleLogger : ILogger
    {
        public void WriteMessage(string messageToLog)
        {
            Console.WriteLine(messageToLog);
        }
    }
}
