using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class CommandService : ICommandService
    {
        private CommandResultProcessor _commandResultProcessor;
        private ICommandRoutingKeyProvider _commandRouteKeyProvider;
        private ITopicProvider<ICommand> _commandTopicProvider;
        private IOHelper _ioHelper;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private Producer<string, string> _producer;
        private SendQueueMessageService _sendMessageService;
        private ITypeNameProvider _typeNameProvider;

        public string CommandExecutedMessageTopic { get; private set; }

        public string DomainEventHandledMessageTopic { get; private set; }

        public CommandResult Execute(ICommand command, int timeoutMillis)
        {
            var result = ExecuteAsync(command).WaitResult(timeoutMillis);
            if (result == null)
            {
                throw new CommandExecuteTimeoutException("Command execute timeout, commandId: {0}, aggregateRootId: {1}", command.Id, command.AggregateRootId);
            }
            return result.Data;
        }

        public CommandResult Execute(ICommand command, CommandReturnType commandReturnType, int timeoutMillis)
        {
            var result = ExecuteAsync(command, commandReturnType).WaitResult(timeoutMillis);
            if (result == null)
            {
                throw new CommandExecuteTimeoutException("Command execute timeout, commandId: {0}, aggregateRootId: {1}", command.Id, command.AggregateRootId);
            }
            return result.Data;
        }

        public Task<AsyncTaskResult<CommandResult>> ExecuteAsync(ICommand command)
        {
            return ExecuteAsync(command, CommandReturnType.CommandExecuted);
        }

        public async Task<AsyncTaskResult<CommandResult>> ExecuteAsync(ICommand command, CommandReturnType commandReturnType)
        {
            try
            {
                Ensure.NotNull(_commandResultProcessor, "commandResultProcessor");
                var taskCompletionSource = new TaskCompletionSource<AsyncTaskResult<CommandResult>>();
                _commandResultProcessor.RegisterProcessingCommand(command, commandReturnType, taskCompletionSource);

                var result = await _sendMessageService.SendMessageAsync(_producer, BuildCommandMessage(command, true), _commandRouteKeyProvider.GetRoutingKey(command), command.Id, null).ConfigureAwait(false);
                if (result.Status == AsyncTaskStatus.Success)
                {
                    return await taskCompletionSource.Task.ConfigureAwait(false);
                }
                _commandResultProcessor.ProcessFailedSendingCommand(command);
                return new AsyncTaskResult<CommandResult>(result.Status, result.ErrorMessage);
            }
            catch (Exception ex)
            {
                return new AsyncTaskResult<CommandResult>(AsyncTaskStatus.Failed, ex.Message);
            }
        }

        public CommandService InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _commandTopicProvider = ObjectContainer.Resolve<ITopicProvider<ICommand>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _commandRouteKeyProvider = ObjectContainer.Resolve<ICommandRoutingKeyProvider>();
            _sendMessageService = new SendQueueMessageService();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            return this;
        }

        public CommandService InitializeKafka(CommandResultProcessor commandResultProcessor = null, Dictionary<string, object> kafkaConfig = null)
        {
            InitializeENode();
            _commandResultProcessor = commandResultProcessor;
            _producer = new Producer<string, string>(kafkaConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));
            return this;
        }

        public void Send(ICommand command)
        {
            _sendMessageService.SendMessage(_producer, BuildCommandMessage(command, false), _commandRouteKeyProvider.GetRoutingKey(command), command.Id, null);
        }

        public Task<AsyncTaskResult> SendAsync(ICommand command)
        {
            try
            {
                return _sendMessageService.SendMessageAsync(_producer, BuildCommandMessage(command, false), _commandRouteKeyProvider.GetRoutingKey(command), command.Id, null);
            }
            catch (Exception ex)
            {
                return Task.FromResult(new AsyncTaskResult(AsyncTaskStatus.Failed, ex.Message));
            }
        }

        public CommandService Shutdown()
        {
            _producer.Dispose();
            if (_commandResultProcessor != null)
            {
                _commandResultProcessor.Shutdown();
            }
            return this;
        }

        public CommandService Start()
        {
            if (_commandResultProcessor != null)
            {
                _commandResultProcessor.Start();
            }
            return this;
        }

        private KafkaMessage BuildCommandMessage(ICommand command, bool needReply = false)
        {
            Ensure.NotNull(command.AggregateRootId, "aggregateRootId");
            var commandData = _jsonSerializer.Serialize(command);
            var topic = _commandTopicProvider.GetTopic(command);
            var replyAddress = needReply && _commandResultProcessor != null ? _commandResultProcessor.BindingAddress.ToString() : null;
            var messageData = _jsonSerializer.Serialize(new CommandMessage
            {
                CommandData = commandData,
                ReplyAddress = replyAddress
            });
            return new KafkaMessage(
                topic,
                (int)KafkaMessageTypeCode.CommandMessage,
                Encoding.UTF8.GetBytes(messageData),
                _typeNameProvider.GetTypeName(command.GetType()));
        }
    }
}