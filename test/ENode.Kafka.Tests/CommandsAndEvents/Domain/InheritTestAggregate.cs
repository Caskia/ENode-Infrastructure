namespace ENode.Kafka.Tests.CommandsAndEvents.Domain
{
    public class InheritTestAggregate : TestAggregate
    {
        public InheritTestAggregate(string id, string title) : base(id, title)
        {
        }

        protected InheritTestAggregate()
        {
        }

        public void ChangeMyTitle(string title)
        {
            ApplyEvent(new TestAggregateTitleChanged(title));
        }
    }
}