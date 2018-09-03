using Confluent.Kafka;

namespace ENode.Kafka.Consumers
{
    public interface IMessageHandler<TKey, TValue>
    {
        void Handle(ConsumeResult<TKey, TValue> message, IMessageContext<TKey, TValue> context);
    }
}