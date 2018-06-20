using ENode.Commanding;

namespace ENode.Kafka.Tests.CommandsAndEvents.Providers
{
    public class CommandTopicProvider : AbstractTopicProvider<ICommand>
    {
        public override string GetTopic(ICommand command)
        {
            return "CommandTopic";
        }
    }
}