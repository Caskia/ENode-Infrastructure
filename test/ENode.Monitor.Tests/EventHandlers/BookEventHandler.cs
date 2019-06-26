using ECommon.Components;
using ECommon.IO;
using ENode.Infrastructure;
using ENode.Monitor.Tests.Domain;
using System.Threading.Tasks;

namespace ENode.Monitor.Tests.EventHandlers
{
    public class BookEventHandler :
        IMessageHandler<BookCreatedEvent>,
        IMessageHandler<BookNameChangedEvent>
    {
        public async Task<AsyncTaskResult> HandleAsync(BookCreatedEvent message)
        {
            await Task.Delay(500);

            return AsyncTaskResult.Success;
        }

        public async Task<AsyncTaskResult> HandleAsync(BookNameChangedEvent message)
        {
            await Task.Delay(500);

            return AsyncTaskResult.Success;
        }
    }
}