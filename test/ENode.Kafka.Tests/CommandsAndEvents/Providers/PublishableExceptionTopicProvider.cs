using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Providers
{
    public class PublishableExceptionTopicProvider : AbstractTopicProvider<IPublishableException>
    {
        public override string GetTopic(IPublishableException source)
        {
            return "PublishableExceptionTopic";
        }
    }
}