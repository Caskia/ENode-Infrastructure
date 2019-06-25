using ENode.Domain;
using System.Threading;

namespace ENode.Monitor.Tests.Domain
{
    public class Book : AggregateRoot<string>
    {
        #region Fields

        private string _name;

        #endregion Fields

        public Book(string id, string name)
            : base(id)
        {
            ApplyEvent(new BookCreatedEvent(name));
        }

        #region Methods

        public void ChangeName(string name)
        {
            Thread.Sleep(1000);
            ApplyEvent(new BookNameChangedEvent(name));
        }

        #endregion Methods

        private void Handle(BookCreatedEvent @event)
        {
            _name = @event.Name;
        }

        private void Handle(BookNameChangedEvent @event)
        {
            _name = @event.Name;
        }
    }
}