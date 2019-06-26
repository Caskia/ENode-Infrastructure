using ENode.Eventing.Impl;
using System.Collections.Generic;

namespace ENode.Monitor.Eventing
{
    public interface IEventMailboxMonitor
    {
        List<EventMailBox> GetProcessingMailboxes(int limit = 10);
    }
}