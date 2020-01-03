namespace ENode.Eventing
{
    public class ProcessingEvent
    {
        public ProcessingEvent(DomainEventStreamMessage message, IEventProcessContext processContext)
        {
            Message = message;
            ProcessContext = processContext;
        }

        public ProcessingEventMailBox MailBox { get; set; }

        public DomainEventStreamMessage Message { get; private set; }

        public IEventProcessContext ProcessContext { get; private set; }

        public void Complete()
        {
            ProcessContext.NotifyEventProcessed();
            if (MailBox != null)
            {
                MailBox.CompleteRun();
            }
        }
    }
}