using ECommon.Components;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ENode.Kafka.Tests.Netty
{
    [Component]
    public class ClientMessageBox : MessageBox
    {
    }

    [Component]
    public class MessageBox
    {
        private IList<object> messages = new List<object>();

        public Task AddAsync(object message)
        {
            if (message == null)
            {
                throw new ArgumentNullException(nameof(message));
            }
            messages.Add(message);
            return Task.CompletedTask;
        }

        public Task<IList<object>> GetAllAsync()
        {
            return Task.FromResult(messages);
        }
    }

    [Component]
    public class ServerMessageBox : MessageBox
    {
    }
}