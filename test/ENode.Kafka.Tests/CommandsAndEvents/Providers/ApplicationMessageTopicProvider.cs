using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Providers
{
    public class ApplicationMessageTopicProvider : AbstractTopicProvider<IApplicationMessage>
    {
        public override string GetTopic(IApplicationMessage source)
        {
            return "ApplicationMessageTopic";
        }
    }
}