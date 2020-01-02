using ENode.Domain;
using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Providers
{
    public class DomainExceptionTopicProvider : AbstractTopicProvider<IDomainException>
    {
        public override string GetTopic(IDomainException source)
        {
            return "DomainExceptionTopic";
        }
    }
}