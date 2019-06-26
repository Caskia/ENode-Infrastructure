using ENode.Commanding;
using ENode.Commanding.Impl;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace ENode.Monitor.Commanding
{
    public class ProcessingCommandMailboxMonitor : IProcessingCommandMailboxMonitor
    {
        private readonly ICommandProcessor _commandProcessor;

        public ProcessingCommandMailboxMonitor(ICommandProcessor commandProcessor)
        {
            _commandProcessor = commandProcessor;
        }

        private ConcurrentDictionary<string, ProcessingCommandMailbox> ProcessingCommandMailboxes
        {
            get
            {
                return typeof(DefaultCommandProcessor)
                     .GetField("_mailboxDict", BindingFlags.NonPublic | BindingFlags.Instance)
                     .GetValue(_commandProcessor) as ConcurrentDictionary<string, ProcessingCommandMailbox>;
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