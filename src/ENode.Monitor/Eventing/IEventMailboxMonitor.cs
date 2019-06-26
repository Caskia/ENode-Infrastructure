using System.Collections.Generic;

namespace ENode.Monitor.Eventing
{
    public interface IEventMailboxMonitor
    {
        List<(string aggregateId, long unConsumedMessageCount)> GetProcessingMailboxesInfo(int limit = 10);
    }
}