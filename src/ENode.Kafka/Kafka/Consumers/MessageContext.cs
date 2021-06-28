using Confluent.Kafka;
using System;

namespace ENode.Kafka.Consumers
{
    public class MessageContext : IMessageContext<Ignore, string>
    {
        public MessageContext(Action<ConsumeResult<Ignore, string>> messageHandledAction)
        {
            MessageHandledAction = messageHandledAction;
        }

        public Action<ConsumeResult<Ignore, string>> MessageHandledAction { get; private set; }

        public void OnMessageHandled(ConsumeResult<Ignore, string> kafkaMessage)
        {
            MessageHandledAction(kafkaMessage);
        }
    }
}