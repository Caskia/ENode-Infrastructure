using Confluent.Kafka;
using System.Threading.Tasks;

namespace ENode.Kafka.Consumers
{
    public interface IMessageHandler<TKey, TValue>
    {
        Task HandleAsync(ConsumeResult<TKey, TValue> message, IMessageContext<TKey, TValue> context);
    }
}