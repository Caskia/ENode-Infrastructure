using ECommon.IO;
using ENode.Infrastructure;
using ENode.Kafka.Tests.CommandsAndEvents.Domain;
using System.Threading.Tasks;

namespace ENode.Kafka.Tests.CommandsAndEvents.EventHandlers
{
    public class TestAggregateEventHandler : IMessageHandler<TestAggregateCreated>
    {
        public Task<AsyncTaskResult> HandleAsync(TestAggregateCreated evnt)
        {
            //DO NOTHING
            return Task.FromResult(AsyncTaskResult.Success);
        }
    }
}