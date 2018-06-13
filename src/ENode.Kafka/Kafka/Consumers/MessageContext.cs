using Confluent.Kafka;
using System;

namespace ENode.Kafka.Consumers
{
    public class MessageContext : IMessageContext<Ignore, string>
    {
        public MessageContext(Action<Message<Ignore, string>> messageHandledAction)
        {
            MessageHandledAction = messageHandledAction;
        }

        public Action<Message<Ignore, string>> MessageHandledAction { get; private set; }

        public void OnMessageHandled(Message<Ignore, string> queueMessage)
        {
            MessageHandledAction(queueMessage);
        }
    }
}