using ENode.Eventing;
using System;

namespace ENode.Monitor.Tests.Domain
{
    [Serializable]
    public class BookCreatedEvent : DomainEvent<string>
    {
        public BookCreatedEvent(string name)
        {
            Name = name;
        }

        protected BookCreatedEvent()
        {
        }

        public string Name { get; private set; }
    }
}