using Confluent.Kafka;

namespace ENode.Kafka.Consumers
{
    public interface IMessageContext<TKey, TValue>
    {
        void OnMessageHandled(ConsumeResult<TKey, TValue> queueMessage);
    }
}