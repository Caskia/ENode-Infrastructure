using ENode.Commanding;
using ENode.Monitor.Reflection;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace ENode.Monitor.Commanding
{
    public class CommandMailboxMonitor : ICommandMailboxMonitor
    {
        private readonly ICommandProcessor _commandProcessor;

        public CommandMailboxMonitor(ICommandProcessor commandProcessor)
        {
            _commandProcessor = commandProcessor;
        }

        private ConcurrentDictionary<string, ProcessingCommandMailbox> ProcessingCommandMailboxes
        {
            get
            {
                return _commandProcessor.GetFieldValue<ConcurrentDictionary<string, ProcessingCommandMailbox>>("_mailboxDict");
            }
        }

        public List<ProcessingCommandMailbox> GetProcessingMailboxes(int limit = 10)
        {
            if (limit > 50)
            {
                throw new System.ArgumentException("can not greater than 50.", nameof(limit));
            }

            return ProcessingCommandMailboxes
                .Select(m => m.Value)
                .OrderByDescending(m => m.TotalUnConsumedMessageCount)
                .Take(limit)
                .ToList();
        }
    }
}