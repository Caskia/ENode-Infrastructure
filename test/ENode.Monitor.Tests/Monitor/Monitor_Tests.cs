using ECommon.Components;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Monitor.Commanding;
using ENode.Monitor.Tests.Commands;
using Shouldly;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace ENode.Monitor.Tests.Monitor
{
    public class Monitor_Tests : MonitorTestBase
    {
        private readonly IProcessingCommandMailboxMonitor _processingCommandMailboxMonitor;

        public Monitor_Tests()
        {
            _processingCommandMailboxMonitor = ObjectContainer.Resolve<IProcessingCommandMailboxMonitor>();
        }

        [Fact]
        public void GetProcessingCommandMailboxInfo_Test()
        {
            //Arrange
            var createCommand = new CreateBook(ObjectId.GenerateNewStringId())
            {
                Name = ObjectId.GenerateNewStringId()
            };
            var processableCreateCommand = new ProcessingCommand(createCommand, new CommandExecuteContext(), new Dictionary<string, string>());
            ProcessCommand(processableCreateCommand);

            var processableChangeNameCommands = new List<ProcessingCommand>();
            for (int i = 0; i < 100; i++)
            {
                processableChangeNameCommands.Add(new ProcessingCommand(
                    new ChangeBookName(createCommand.AggregateRootId)
                    {
                        Name = ObjectId.GenerateNewStringId()
                    },
                    new CommandExecuteContext(),
                    new Dictionary<string, string>()));
            }
            processableChangeNameCommands.ForEach(c =>
            {
                ProcessCommand(c);
            });

            //Act
            var mailboxes = _processingCommandMailboxMonitor.GetProcessingMailboxes();

            //Assert
            mailboxes.Any(m => m.TotalUnConsumedMessageCount > 0).ShouldBeTrue();
        }
    }
}