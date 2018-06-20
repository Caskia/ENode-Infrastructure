using ENode.Eventing;

namespace ENode.Kafka.Tests.CommandsAndEvents.Providers
{
    public class EventTopicProvider : AbstractTopicProvider<IDomainEvent>
    {
        public override string GetTopic(IDomainEvent source)
        {
            return "EventTopic";
        }
    }
}