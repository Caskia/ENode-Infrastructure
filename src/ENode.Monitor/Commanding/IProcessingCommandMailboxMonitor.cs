using ENode.Commanding;
using System.Collections.Generic;

namespace ENode.Monitor.Commanding
{
    public interface IProcessingCommandMailboxMonitor
    {
        List<ProcessingCommandMailbox> GetProcessingMailboxes(int limit = 10);
    }
}