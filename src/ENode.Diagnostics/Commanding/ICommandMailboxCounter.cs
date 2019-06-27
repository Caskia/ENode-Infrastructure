using System.Collections.Generic;

namespace ENode.Diagnostics.Commanding
{
    public interface ICommandMailboxCounter
    {
        List<(string aggregateId, long unConsumedMessageCount)> GetProcessingMailboxesInfo(int limit = 10);
    }
}