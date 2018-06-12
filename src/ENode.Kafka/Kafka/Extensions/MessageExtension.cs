using Confluent.Kafka;

namespace ENode.Kafka.Extensions
{
    public static class MessageExtension
    {
        public static string ToKeyString(this Message<Ignore, string> message)
        {
            return $"[Topic={message.Topic}, Partition={message.Partition}]";
        }
    }
}