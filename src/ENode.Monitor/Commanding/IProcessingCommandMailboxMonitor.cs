using ENode.Commanding;
using System.Collections.Generic;

namespace ENode.Monitor.Commanding
{
    public interface IProcessingCommandMailboxMonitor
    {
        List<ProcessingCommandMailbox> GetAllMailboxes();
    }
}