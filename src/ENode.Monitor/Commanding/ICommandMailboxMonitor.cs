using System.Collections.Generic;

namespace ENode.Monitor.Commanding
{
    public interface ICommandMailboxMonitor
    {
        List<(string aggregateId, long unConsumedMessageCount)> GetProcessingMailboxesInfo(int limit = 10);
    }
}