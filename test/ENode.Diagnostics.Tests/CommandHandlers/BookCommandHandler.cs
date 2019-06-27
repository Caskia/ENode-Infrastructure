using ENode.Commanding;
using ENode.Diagnostics.Tests.Commands;
using ENode.Diagnostics.Tests.Domain;
using System.Threading.Tasks;

namespace ENode.Diagnostics.Tests.CommandHandlers
{
    public class BookCommandHandler :
        ICommandHandler<CreateBook>,
        ICommandHandler<ChangeBookName>
    {
        public Task HandleAsync(ICommandContext context, CreateBook command)
        {
            return context.AddAsync(new Book(command.AggregateRootId, command.Name));
        }

        public async Task HandleAsync(ICommandContext context, ChangeBookName command)
        {
            (await context.GetAsync<Book>(command.AggregateRootId))
                .ChangeName(command.Name);
        }
    }
}