using System.Collections.Generic;

namespace ENode.Diagnostics.Eventing
{
    public interface IEventMailboxCounter
    {
        List<(string aggregateId, long unConsumedMessageCount)> GetProcessingMailboxesInfo(int limit = 10);
    }
}