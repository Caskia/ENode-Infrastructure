using System;
using System.Collections.Generic;
using ENode.Domain;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Domain
{
    public class Event1 : DomainEvent<string> { }

    public class Event2 : DomainEvent<string> { }

    public class Event3 : DomainEvent<string> { }

    public class TestAggregate : AggregateRoot<string>
    {
        private string _title;

        public TestAggregate(string id, string title) : base(id)
        {
            ApplyEvent(new TestAggregateCreated(title));
        }

        protected TestAggregate()
        {
        }

        public string Title { get { return _title; } }

        public void ChangeTitle(string title)
        {
            ApplyEvent(new TestAggregateTitleChanged(title));
        }

        public void TestEvents()
        {
            ApplyEvents(new Event1(), new Event2(), new Event3());
        }

        public void ThrowException(bool isDomainException)
        {
            if (isDomainException)
            {
                throw new TestDomainException(Id);
            }
            else
            {
                throw new Exception("TestException");
            }
        }

        private void Handle(TestAggregateCreated evnt)
        {
            _title = evnt.Title;
        }

        private void Handle(TestAggregateTitleChanged evnt)
        {
            _title = evnt.Title;
        }

        private void Handle(Event1 evnt)
        {
        }

        private void Handle(Event2 evnt)
        {
        }

        private void Handle(Event3 evnt)
        {
        }
    }

    public class TestAggregateCreated : DomainEvent<string>
    {
        public TestAggregateCreated()
        {
        }

        public TestAggregateCreated(string title)
        {
            Title = title;
        }

        public string Title { get; private set; }
    }

    public class TestAggregateTitleChanged : DomainEvent<string>
    {
        public TestAggregateTitleChanged()
        {
        }

        public TestAggregateTitleChanged(string title)
        {
            Title = title;
        }

        public string Title { get; private set; }
    }

    public class TestDomainException : DomainException
    {
        public TestDomainException(string aggregateRootId) : base()
        {
            AggregateRootId = aggregateRootId;
        }

        public string AggregateRootId { get; private set; }

        public override void RestoreFrom(IDictionary<string, string> serializableInfo)
        {
            AggregateRootId = serializableInfo["AggregateRootId"];
        }

        public override void SerializeTo(IDictionary<string, string> serializableInfo)
        {
            serializableInfo.Add("AggregateRootId", AggregateRootId);
        }
    }
}