using ECommon.Components;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Domain;
using ENode.Eventing;
using ENode.Kafka.Tests.CommandsAndEvents.Commands;
using ENode.Kafka.Tests.CommandsAndEvents.Domain;
using ENode.Kafka.Tests.CommandsAndEvents.Tests;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace ENode.Kafka.Tests.CommandsAndEvents
{
    public class CommandAndEventServiceTest : BaseTest
    {
        public readonly static ConcurrentDictionary<int, IList<string>> HandlerTypes = new ConcurrentDictionary<int, IList<string>>();

        #region Command Tests

        [Fact]
        public async Task aggregate_throw_exception_command_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            await _commandService.ExecuteAsync(command);

            var command1 = new AggregateThrowExceptionCommand
            {
                AggregateRootId = aggregateId,
                IsDomainException = false
            };
            var commandResult = await _commandService.ExecuteAsync(command1);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);

            var command2 = new AggregateThrowExceptionCommand
            {
                AggregateRootId = aggregateId,
                IsDomainException = true
            };
            commandResult = await _commandService.ExecuteAsync(command2);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
        }

        [Fact]
        public async Task change_multiple_aggregates_test()
        {
            var command1 = new CreateTestAggregateCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId(),
                Title = "Sample Note1"
            };
            await _commandService.ExecuteAsync(command1);

            var command2 = new CreateTestAggregateCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId(),
                Title = "Sample Note2"
            };
            await _commandService.ExecuteAsync(command2);

            var command3 = new ChangeMultipleAggregatesCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId(),
                AggregateRootId1 = command1.AggregateRootId,
                AggregateRootId2 = command2.AggregateRootId
            };
            var commandResult = await _commandService.ExecuteAsync(command3);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
        }

        [Fact]
        public async Task change_nothing_test()
        {
            var command = new ChangeNothingCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.NothingChanged, commandResult.Status);
        }

        [Fact]
        public async Task command_inheritance_test()
        {
            var command = new BaseCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.NothingChanged, commandResult.Status);
            Assert.Equal("ResultFromBaseCommand", commandResult.Result);

            command = new ChildCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.NothingChanged, commandResult.Status);
            Assert.Equal("ResultFromChildCommand", commandResult.Result);
        }

        [Fact]
        public async Task command_sync_execute_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note",
                SleepMilliseconds = 3000
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //执行修改聚合根的命令
            var command2 = new ChangeTestAggregateTitleCommand
            {
                AggregateRootId = aggregateId,
                Title = "Changed Note"
            };
            commandResult = await _commandService.ExecuteAsync(command2);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Changed Note", note.Title);
            Assert.Equal(2, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task create_and_concurrent_update_aggregate_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //并发执行修改聚合根的命令
            var totalCount = 100;
            var finishedCount = 0;
            var waitHandle = new ManualResetEvent(false);
            for (var i = 0; i < totalCount; i++)
            {
                var updateCommand = new ChangeTestAggregateTitleCommand
                {
                    AggregateRootId = aggregateId,
                    Title = "Changed Note"
                };
                await _commandService.ExecuteAsync(updateCommand).ContinueWith(async t =>
                {
                    var result = t.Result;
                    Assert.NotNull(result);
                    Assert.Equal(CommandStatus.Success, result.Status);

                    var current = Interlocked.Increment(ref finishedCount);
                    if (current == totalCount)
                    {
                        note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
                        Assert.NotNull(note);
                        Assert.Equal("Changed Note", note.Title);
                        Assert.Equal(totalCount + 1, ((IAggregateRoot)note).Version);
                        waitHandle.Set();
                    }
                });
            }
            waitHandle.WaitOne();
        }

        [Fact]
        public async Task create_and_update_aggregate_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //执行修改聚合根的命令
            var command2 = new ChangeTestAggregateTitleCommand
            {
                AggregateRootId = aggregateId,
                Title = "Changed Note"
            };
            commandResult = await _commandService.ExecuteAsync(command2);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Changed Note", note.Title);
            Assert.Equal(2, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task create_and_update_inherit_aggregate_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateInheritTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<InheritTestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //执行修改聚合根的命令
            var command2 = new ChangeInheritTestAggregateTitleCommand
            {
                AggregateRootId = aggregateId,
                Title = "Changed Note"
            };
            commandResult = await _commandService.ExecuteAsync(command2);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            note = await _memoryCache.GetAsync<InheritTestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Changed Note", note.Title);
            Assert.Equal(2, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task duplicate_create_aggregate_command_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //用同一个命令再次执行创建聚合根的命令
            commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //用另一个命令再次执行创建相同聚合根的命令
            command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };
            commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task duplicate_update_aggregate_command_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command1 = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //先创建一个聚合根
            var status = (await _commandService.ExecuteAsync(command1)).Status;
            Assert.Equal(CommandStatus.Success, status);

            var command2 = new ChangeTestAggregateTitleCommand
            {
                AggregateRootId = aggregateId,
                Title = "Changed Note"
            };

            //执行修改聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command2, CommandReturnType.EventHandled);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Changed Note", note.Title);
            Assert.Equal(2, ((IAggregateRoot)note).Version);

            //在重复执行该命令
            commandResult = await _commandService.ExecuteAsync(command2);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Changed Note", note.Title);
            Assert.Equal(2, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task handler_throw_exception_command_test()
        {
            var command = new ThrowExceptionCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
        }

        [Fact]
        public async Task no_handler_command_test()
        {
            var command = new NoHandlerCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
        }

        [Fact]
        public async Task send_concurrent_command_test()
        {
            ThreadPool.SetMinThreads(1, 1);
            ThreadPool.SetMaxThreads(2, 2);

            var tasks = new List<Task<CommandResult>>();

            for (int i = 0; i < 100; i++)
            {
                var aggregateId = ObjectId.GenerateNewStringId();
                var command = new CreateTestAggregateCommand
                {
                    AggregateRootId = aggregateId,
                    Title = "Sample Note"
                };

                tasks.Add(_commandService.ExecuteAsync(command, CommandReturnType.EventHandled));
            }

            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task set_application_message_command_handler_test()
        {
            var command = new SetApplicatonMessageCommand()
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
        }

        [Fact]
        public async Task set_result_command_test()
        {
            var command = new SetResultCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId(),
                Result = "CommandResult"
            };
            var commandResult = await _commandService.ExecuteAsync(command, CommandReturnType.EventHandled);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            Assert.Equal("CommandResult", commandResult.Result);
        }

        [Fact]
        public async Task two_handlers_command_test()
        {
            var command = new TwoHandlersCommand
            {
                AggregateRootId = ObjectId.GenerateNewStringId()
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Failed, commandResult.Status);
        }

        #endregion Command Tests

        #region Event Service Tests

        [Fact]
        public async Task create_concurrent_conflict_and_then_update_many_times_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var commandId = ObjectId.GenerateNewStringId();

            //往EventStore直接插入事件，用于模拟并发冲突的情况
            var eventStream = new DomainEventStream(
                commandId,
                aggregateId,
                typeof(TestAggregate).FullName,
                DateTime.Now,
                new IDomainEvent[] { new TestAggregateCreated("Note Title") { AggregateRootId = aggregateId, Version = 1 } },
                null);
            var result = await _eventStore.BatchAppendAsync(new DomainEventStream[] { eventStream });
            Assert.NotNull(result);
            Assert.Equal(aggregateId, result.SuccessAggregateRootIdList[0]);
            _logger.Info("----create_concurrent_conflict_and_then_update_many_times_test, _eventStore.AppendAsync success");

            await _publishedVersionStore.UpdatePublishedVersionAsync("DefaultEventProcessor", typeof(TestAggregate).FullName, aggregateId, 1);
            _logger.Info("----create_concurrent_conflict_and_then_update_many_times_test, UpdatePublishedVersionAsync success");

            //执行创建聚合根的命令
            var command = new CreateTestAggregateCommand
            {
                Id = commandId,
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };
            var commandResult = await _commandService.ExecuteAsync(command);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            _logger.Info("----create_concurrent_conflict_and_then_update_many_times_test, _commandService.ExecuteAsync create success");

            var commandList = new List<ICommand>();
            for (var i = 0; i < 50; i++)
            {
                commandList.Add(new ChangeTestAggregateTitleCommand
                {
                    AggregateRootId = aggregateId,
                    Title = "Changed Note Title"
                });
            }

            var waitHandle = new ManualResetEvent(false);
            var count = 0L;
            foreach (var updateCommand in commandList)
            {
                await _commandService.ExecuteAsync(updateCommand).ContinueWith(t =>
                {
                    Assert.NotNull(t.Result);
                    var updateCommandResult = t.Result;
                    Assert.NotNull(updateCommandResult);
                    Assert.Equal(CommandStatus.Success, updateCommandResult.Status);
                    var totalCount = Interlocked.Increment(ref count);
                    _logger.InfoFormat("----create_concurrent_conflict_and_then_update_many_times_test, updateCommand finished, count: {0}", totalCount);
                    if (totalCount == commandList.Count)
                    {
                        waitHandle.Set();
                    }
                });
            }
            waitHandle.WaitOne();
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal(commandList.Count + 1, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task create_concurrent_conflict_and_then_update_many_times_test2()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var commandId = ObjectId.GenerateNewStringId();

            //往EventStore直接插入事件，用于模拟并发冲突的情况
            var eventStream = new DomainEventStream(
                commandId,
                aggregateId,
                typeof(TestAggregate).FullName,
                DateTime.Now,
                new IDomainEvent[] { new TestAggregateCreated("Note Title") { AggregateRootId = aggregateId, Version = 1 } },
                null);
            var result = await _eventStore.BatchAppendAsync(new DomainEventStream[] { eventStream });
            Assert.NotNull(result);
            Assert.Equal(aggregateId, result.SuccessAggregateRootIdList[0]);

            await _publishedVersionStore.UpdatePublishedVersionAsync("DefaultEventProcessor", typeof(TestAggregate).FullName, aggregateId, 1);

            var commandList = new List<ICommand>();
            commandList.Add(new CreateTestAggregateCommand
            {
                Id = commandId,
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            });
            for (var i = 0; i < 50; i++)
            {
                commandList.Add(new ChangeTestAggregateTitleCommand
                {
                    AggregateRootId = aggregateId,
                    Title = "Changed Note Title"
                });
            }

            var waitHandle = new ManualResetEvent(false);
            var count = 0L;
            var createCommandSuccess = false;
            foreach (var updateCommand in commandList)
            {
                await _commandService.ExecuteAsync(updateCommand).ContinueWith(t =>
                {
                    Assert.NotNull(t.Result);
                    var commandResult = t.Result;
                    Assert.NotNull(commandResult);
                    Assert.Equal(CommandStatus.Success, commandResult.Status);
                    if (commandResult.CommandId != commandId)
                    {
                        var totalCount = Interlocked.Increment(ref count);
                        if (totalCount == commandList.Count - 1)
                        {
                            waitHandle.Set();
                        }
                    }
                    else
                    {
                        createCommandSuccess = true;
                    }
                });
            }
            waitHandle.WaitOne();
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.True(createCommandSuccess);
            Assert.Equal(commandList.Count, ((IAggregateRoot)note).Version);
        }

        [Fact]
        public async Task update_concurrent_conflict_test()
        {
            var aggregateId = ObjectId.GenerateNewStringId();
            var command = new CreateTestAggregateCommand
            {
                AggregateRootId = aggregateId,
                Title = "Sample Note"
            };

            //执行创建聚合根的命令
            var commandResult = await _commandService.ExecuteAsync(command, CommandReturnType.EventHandled);
            Assert.NotNull(commandResult);
            Assert.Equal(CommandStatus.Success, commandResult.Status);
            var note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal("Sample Note", note.Title);
            Assert.Equal(1, ((IAggregateRoot)note).Version);

            //往EventStore直接插入事件，用于模拟并发冲突的情况
            var eventStream = new DomainEventStream(
                ObjectId.GenerateNewStringId(),
                aggregateId,
                typeof(TestAggregate).FullName,
                DateTime.Now,
                new IDomainEvent[] { new TestAggregateTitleChanged("Changed Title") { AggregateRootId = aggregateId, Version = 2 } },
                null);
            var result = await _eventStore.BatchAppendAsync(new DomainEventStream[] { eventStream });
            Assert.NotNull(result);
            Assert.Equal(aggregateId, result.SuccessAggregateRootIdList[0]);

            await _publishedVersionStore.UpdatePublishedVersionAsync("DefaultEventProcessor", typeof(TestAggregate).FullName, aggregateId, 2);

            var commandList = new List<ICommand>();
            for (var i = 0; i < 50; i++)
            {
                commandList.Add(new ChangeTestAggregateTitleCommand
                {
                    AggregateRootId = aggregateId,
                    Title = "Changed Note2"
                });
            }

            var waitHandle = new ManualResetEvent(false);
            var count = 0L;
            foreach (var updateCommand in commandList)
            {
                await _commandService.ExecuteAsync(updateCommand).ContinueWith(t =>
                {
                    Assert.NotNull(t.Result);
                    var currentCommandResult = t.Result;
                    Assert.NotNull(currentCommandResult);
                    Assert.Equal(CommandStatus.Success, currentCommandResult.Status);
                    var totalCount = Interlocked.Increment(ref count);
                    if (totalCount == commandList.Count)
                    {
                        waitHandle.Set();
                    }
                });
            }
            waitHandle.WaitOne();
            note = await _memoryCache.GetAsync<TestAggregate>(aggregateId);
            Assert.NotNull(note);
            Assert.Equal(2 + commandList.Count, ((IAggregateRoot)note).Version);
            Assert.Equal("Changed Note2", note.Title);
        }

        #endregion Event Service Tests

        [Fact]
        public async Task event_handler_priority_test()
        {
            var noteId = ObjectId.GenerateNewStringId();
            var command1 = new CreateTestAggregateCommand { AggregateRootId = noteId, Title = "Sample Title1" };
            var command2 = new TestEventPriorityCommand { AggregateRootId = noteId };
            var commandResult1 = await _commandService.ExecuteAsync(command1, CommandReturnType.EventHandled);
            var commandResult2 = await _commandService.ExecuteAsync(command2, CommandReturnType.EventHandled);

            Thread.Sleep(3000);

            Assert.Equal(CommandStatus.Success, commandResult1.Status);
            Assert.Equal(CommandStatus.Success, commandResult2.Status);

            Assert.Equal(3, HandlerTypes[1].Count);
            Assert.Equal(typeof(Handler3).Name, HandlerTypes[1][0]);
            Assert.Equal(typeof(Handler2).Name, HandlerTypes[1][1]);
            Assert.Equal(typeof(Handler1).Name, HandlerTypes[1][2]);

            Assert.Equal(3, HandlerTypes[2].Count);
            Assert.Equal(typeof(Handler122).Name, HandlerTypes[2][0]);
            Assert.Equal(typeof(Handler121).Name, HandlerTypes[2][1]);
            Assert.Equal(typeof(Handler123).Name, HandlerTypes[2][2]);

            Assert.Equal(3, HandlerTypes[3].Count);
            Assert.Equal(typeof(Handler1232).Name, HandlerTypes[3][0]);
            Assert.Equal(typeof(Handler1231).Name, HandlerTypes[3][1]);
            Assert.Equal(typeof(Handler1233).Name, HandlerTypes[3][2]);

            HandlerTypes.Clear();
        }

        [Fact]
        public async Task sequence_domain_event_process_test()
        {
            var processor = ObjectContainer.Resolve<IProcessingEventProcessor>();

            var note = new TestAggregate(ObjectId.GenerateNewStringId(), "initial title");
            var aggregate = note as IAggregateRoot;
            var message1 = CreateMessage(aggregate);

            aggregate.AcceptChanges();
            note.ChangeTitle("title1");
            var message2 = CreateMessage(aggregate);

            aggregate.AcceptChanges();
            note.ChangeTitle("title2");
            var message3 = CreateMessage(aggregate);

            var waitHandle = new ManualResetEvent(false);
            var versionList = new List<int>();

            await processor.ProcessAsync(new ProcessingEvent(message1, new DomainEventStreamProcessContext(message1, waitHandle, versionList)));
            await processor.ProcessAsync(new ProcessingEvent(message3, new DomainEventStreamProcessContext(message3, waitHandle, versionList)));
            await processor.ProcessAsync(new ProcessingEvent(message2, new DomainEventStreamProcessContext(message2, waitHandle, versionList)));

            waitHandle.WaitOne();

            for (var i = 0; i < 3; i++)
            {
                Assert.Equal(i + 1, versionList[i]);
            }
        }

        [Fact]
        public async Task sequence_domain_event_process_test2()
        {
            var processor = ObjectContainer.Resolve<IProcessingEventProcessor>();

            var note = new TestAggregate(ObjectId.GenerateNewStringId(), "initial title");
            var aggregate = note as IAggregateRoot;
            var message1 = CreateMessage(aggregate);

            aggregate.AcceptChanges();
            note.ChangeTitle("title1");
            var message2 = CreateMessage(aggregate);

            aggregate.AcceptChanges();
            note.ChangeTitle("title2");
            var message3 = CreateMessage(aggregate);

            var waitHandle = new ManualResetEvent(false);
            var versionList = new List<int>();

            //模拟从publishedVersionStore中取到的publishedVersion不是最新的场景
            await processor.ProcessAsync(new ProcessingEvent(message1, new DomainEventStreamProcessContext(message1, waitHandle, versionList)));
            await processor.ProcessAsync(new ProcessingEvent(message3, new DomainEventStreamProcessContext(message3, waitHandle, versionList)));

            //等待5秒后更新publishedVersion为2
            Thread.Sleep(5000);
            var publishedVersionStore = ObjectContainer.Resolve<IPublishedVersionStore>();
            publishedVersionStore.UpdatePublishedVersionAsync(processor.Name, typeof(TestAggregate).FullName, aggregate.UniqueId, 2).Wait();

            //等待ENode内部自动检测到最新的publishedVersion，并继续处理mailbox waitingList中的version=3的事件
            waitHandle.WaitOne();

            Assert.Equal(1, versionList[0]);
            Assert.Equal(3, versionList[1]);

            //再等待3秒，等待ENode内部异步打印Removed problem aggregate的日志
            Thread.Sleep(3000);
        }

        private DomainEventStreamMessage CreateMessage(IAggregateRoot aggregateRoot)
        {
            return new DomainEventStreamMessage(
                ObjectId.GenerateNewStringId(),
                aggregateRoot.UniqueId,
                aggregateRoot.Version + 1,
                aggregateRoot.GetType().FullName,
                aggregateRoot.GetChanges(),
                new Dictionary<string, string>());
        }

        private class DomainEventStreamProcessContext : IEventProcessContext
        {
            private DomainEventStreamMessage _domainEventStreamMessage;
            private IList<int> _versionList;
            private ManualResetEvent _waitHandle;

            public DomainEventStreamProcessContext(DomainEventStreamMessage domainEventStreamMessage, ManualResetEvent waitHandle, IList<int> versionList)
            {
                _domainEventStreamMessage = domainEventStreamMessage;
                _waitHandle = waitHandle;
                _versionList = versionList;
            }

            public void NotifyEventProcessed()
            {
                _versionList.Add(_domainEventStreamMessage.Version);
                if (_domainEventStreamMessage.Version == 3)
                {
                    _waitHandle.Set();
                }
            }
        }
    }
}