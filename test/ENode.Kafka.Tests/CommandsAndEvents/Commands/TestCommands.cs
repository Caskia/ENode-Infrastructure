using ENode.Commanding;

namespace ENode.Kafka.Tests.CommandsAndEvents.Commands
{
    public class AggregateThrowExceptionCommand : Command<string>
    {
        public bool PublishableException { get; set; }
    }

    public class AsyncHandlerBaseCommand : Command<string>
    {
    }

    public class AsyncHandlerChildCommand : AsyncHandlerBaseCommand
    {
    }

    public class AsyncHandlerCommand : Command<string>
    {
        public bool ShouldGenerateApplicationMessage { get; set; }
        public bool ShouldThrowException { get; set; }
        public bool ShouldThrowIOException { get; set; }
    }

    public class BaseCommand : Command<string>
    {
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

    public class CreateTestAggregateCommand : Command<string>
    {
        public int SleepMilliseconds { get; set; }
        public string Title { get; set; }
    }

    public class NoHandlerCommand : Command<string>
    {
    }

    public class NotCheckAsyncHandlerExistCommand : Command<string>
    {
    }

    public class NotCheckAsyncHandlerExistWithResultCommand : Command<string>
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

    public class TwoAsyncHandlersCommand : Command<string>
    {
    }

    public class TwoHandlersCommand : Command<string>
    {
    }
}