using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Commanding;
using ENode.Infrastructure;
using ENode.Kafka.Producers;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class CommandService : ICommandService
    {
        private CommandResultProcessor _commandResultProcessor;
        private ITopicProvider<ICommand> _commandTopicProvider;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private SendQueueMessageService _sendMessageService;
        private ITypeNameProvider _typeNameProvider;
        public string CommandExecutedMessageTopic { get; private set; }
        public string DomainEventHandledMessageTopic { get; private set; }
        public Producer Producer { get; private set; }

        public Task<CommandResult> ExecuteAsync(ICommand command)
        {
            return ExecuteAsync(command, CommandReturnType.CommandExecuted);
        }

        public async Task<CommandResult> ExecuteAsync(ICommand command, CommandReturnType commandReturnType)
        {
            Ensure.NotNull(_commandResultProcessor, "commandResultProcessor");
            var taskCompletionSource = new TaskCompletionSource<CommandResult>();
            _commandResultProcessor.RegisterProcessingCommand(command, commandReturnType, taskCompletionSource);

            try
            {
                await _sendMessageService.SendMessageAsync(Producer, "command", command.GetType().Name, BuildCommandMessage(command, true), command.AggregateRootId, command.Id, command.Items).ConfigureAwait(false);
            }
            catch
            {
                _commandResultProcessor.ProcessFailedSendingCommand(command);
                throw;
            }

            return await taskCompletionSource.Task.ConfigureAwait(false);
        }

        public CommandService InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _commandTopicProvider = ObjectContainer.Resolve<ITopicProvider<ICommand>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _sendMessageService = new SendQueueMessageService();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }

        public CommandService InitializeKafka(ProducerSetting setting = null, CommandResultProcessor commandResultProcessor = null)
        {
            InitializeENode();
            _commandResultProcessor = commandResultProcessor;
            Producer = new Producer(setting);
            return this;
        }

        public Task SendAsync(ICommand command)
        {
            return _sendMessageService.SendMessageAsync(Producer, "command", command.GetType().Name, BuildCommandMessage(command, false), command.AggregateRootId, command.Id, command.Items);
        }

        public CommandService Shutdown()
        {
            Producer.Stop();
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
            Producer.OnLog = (_, info) => _logger.Info($"ENode CommandService: {info}");
            Producer.OnError = (_, error) => _logger.Error($"ENode CommandService has an error: {error}");
            Producer.Start();

            return this;
        }

        private ENodeMessage BuildCommandMessage(ICommand command, bool needReply = false)
        {
            Ensure.NotNull(command.AggregateRootId, "aggregateRootId");
            var commandData = _jsonSerializer.Serialize(command);
            var topic = _commandTopicProvider.GetTopic(command);
            var replyAddress = needReply && _commandResultProcessor != null ? _commandResultProcessor.BindingServerAddress : null;
            var messageData = _jsonSerializer.Serialize(new CommandMessage
            {
                CommandData = commandData,
                ReplyAddress = replyAddress
            });
            return new ENodeMessage(
                topic,
                (int)ENodeMessageTypeCode.CommandMessage,
                messageData,
                _typeNameProvider.GetTypeName(command.GetType()));
        }
    }
}