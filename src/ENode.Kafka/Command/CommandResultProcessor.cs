using System;
using System.Net;
using System.Threading.Tasks;
using ECommon.IO;
using ENode.Commanding;

namespace ENode.Kafka
{
    public class CommandResultProcessor
    {
        public EndPoint BindingAddress { get; private set; }

        public void Shutdown()
        {
            throw new NotImplementedException();
        }

        public void Start()
        {
            throw new NotImplementedException();
        }

        internal void ProcessFailedSendingCommand(ICommand command)
        {
            throw new NotImplementedException();
        }

        internal void RegisterProcessingCommand(ICommand command, CommandReturnType commandReturnType, TaskCompletionSource<AsyncTaskResult<CommandResult>> taskCompletionSource)
        {
            throw new NotImplementedException();
        }
    }
}