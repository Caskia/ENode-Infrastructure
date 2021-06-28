using ENode.Kafka.Tests.CommandsAndEvents.Domain;
using ENode.Messaging;
using System.Threading.Tasks;

namespace ENode.Kafka.Tests.CommandsAndEvents.EventHandlers
{
    public class TestAggregateEventHandler : IMessageHandler<TestAggregateCreated>
    {
        public Task HandleAsync(TestAggregateCreated evnt)
        {
            //DO NOTHING
            return Task.CompletedTask;
        }
    }
}