using ENode.Commanding;

namespace ENode.Kafka.Tests.CommandsAndEvents.Commands
{
    public class AggregateThrowExceptionCommand : Command<string>
    {
        public bool IsDomainException { get; set; }
    }

    public class BaseCommand : Command<string>
    {
    }

    public class ChangeInheritTestAggregateTitleCommand : Command<string>
    {
        public string Title { get; set; }
    }

    public class ChangeMultipleAggregatesCommand : Command<string>
    {
        public string AggregateRootId1 { get; set; }
        public string AggregateRootId2 { get; set; }
    }

    public class ChangeNothingCommand : Command<string>
    {
    }

    public class ChangeTestAggregateTitleCommand : Command<string>
    {
        public string Title { get; set; }
    }

    public class ChildCommand : BaseCommand
    {
    }

    public class CreateInheritTestAggregateCommand : Command<string>
    {
        public string Title { get; set; }
    }

    public class CreateTestAggregateCommand : Command<string>
    {
        public int SleepMilliseconds { get; set; }
        public string Title { get; set; }
    }

    public class NoHandlerCommand : Command<string>
    {
    }

    public class SetApplicatonMessageCommand : Command<string>
    {
    }

    public class SetResultCommand : Command<string>
    {
        public string Result { get; set; }
    }

    public class TestEventPriorityCommand : Command<string>
    {
    }

    public class ThrowExceptionCommand : Command<string>
    {
    }

    public class TwoHandlersCommand : Command<string>
    {
    }
}