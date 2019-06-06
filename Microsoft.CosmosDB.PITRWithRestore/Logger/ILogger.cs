using System.Collections.Generic;

namespace Microsoft.CosmosDB.PITRWithRestore.Logger
{
    public interface ILogger
    {
        void WriteMessage(string messageToLog);
    }
}
