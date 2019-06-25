using ECommon.Logging;
using ECommon.Scheduling;
using ENode.Commanding;
using ENode.Commanding.Impl;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

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
    }
}