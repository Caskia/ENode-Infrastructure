using ENode.Eventing;

namespace ENode.Diagnostics.Tests.Domain
{
    public class BookNameChangedEvent : DomainEvent<string>
    {
        public BookNameChangedEvent(string name)
        {
            Name = name;
        }

        protected BookNameChangedEvent()
        {
        }

        public string Name { get; private set; }
    }
}