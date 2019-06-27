using ECommon.Components;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Diagnostics.Commanding;
using ENode.Diagnostics.Eventing;
using ENode.Diagnostics.Tests.Commands;
using Shouldly;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Xunit;

namespace ENode.Diagnostics.Tests.Counters
{
    public class Counter_Tests : DiagnosticsTestBase
    {
        private readonly ICommandMailboxCounter _commandMailboxDiagnostics;
        private readonly IEventMailboxCounter _eventMailboxDiagnostics;

        public Counter_Tests()
        {
            _commandMailboxDiagnostics = ObjectContainer.Resolve<ICommandMailboxCounter>();
            _eventMailboxDiagnostics = ObjectContainer.Resolve<IEventMailboxCounter>();
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
            var mailboxes = _commandMailboxDiagnostics.GetProcessingMailboxesInfo();

            //Assert
            mailboxes.Any().ShouldBeTrue();
        }

        [Fact]
        public void GetProcessingEventMailboxInfo_Test()
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

            Thread.Sleep(2000);

            //Act
            var mailboxes = _eventMailboxDiagnostics.GetProcessingMailboxesInfo();

            //Assert
            mailboxes.Any().ShouldBeTrue();
        }
    }
}