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

        public List<ProcessingCommandMailbox> GetAllMailboxes()
        {
            return ProcessingCommandMailboxes.Select(m => m.Value).ToList();
        }
    }
}