using Confluent.Kafka;

namespace ENode.Kafka.Consumers
{
    public interface IMessageContext<TKey, TValue>
    {
        void OnMessageHandled(Message<TKey, TValue> queueMessage);
    }
}