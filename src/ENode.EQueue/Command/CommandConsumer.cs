using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Domain;
using ENode.Infrastructure;
using EQueue.Clients.Consumers;
using EQueue.Protocols;
using IQueueMessageHandler = EQueue.Clients.Consumers.IMessageHandler;

namespace ENode.EQueue
{
    public class CommandConsumer : IQueueMessageHandler
    {
        private const string DefaultCommandConsumerGroup = "CommandConsumerGroup";
        private IAggregateStorage _aggregateStorage;
        private ICommandProcessor _commandProcessor;
        private Consumer _consumer;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IRepository _repository;
        private SendReplyService _sendReplyService;
        private ITypeNameProvider _typeNameProvider;
        public Consumer Consumer { get { return _consumer; } }

        void IQueueMessageHandler.Handle(QueueMessage queueMessage, IMessageContext context)
        {
            var commandItems = new Dictionary<string, string>();
            var commandMessage = _jsonSerializer.Deserialize<CommandMessage>(Encoding.UTF8.GetString(queueMessage.Body));
            var commandType = _typeNameProvider.GetType(queueMessage.Tag);
            var command = _jsonSerializer.Deserialize(commandMessage.CommandData, commandType) as ICommand;
            var commandExecuteContext = new CommandExecuteContext(_repository, _aggregateStorage, queueMessage, context, commandMessage, _sendReplyService);
            commandItems["CommandReplyAddress"] = commandMessage.ReplyAddress;
            _logger.InfoFormat("ENode command message received, messageId: {0}, aggregateRootId: {1}", command.Id, command.AggregateRootId);
            _commandProcessor.Process(new ProcessingCommand(command, commandExecuteContext, commandItems));
        }

        public CommandConsumer InitializeENode()
        {
            _sendReplyService = new SendReplyService("CommandConsumerSendReplyService");
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _commandProcessor = ObjectContainer.Resolve<ICommandProcessor>();
            _repository = ObjectContainer.Resolve<IRepository>();
            _aggregateStorage = ObjectContainer.Resolve<IAggregateStorage>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }

        public CommandConsumer InitializeEQueue(string groupName = null, ConsumerSetting setting = null)
        {
            InitializeENode();
            _consumer = new Consumer(groupName ?? DefaultCommandConsumerGroup, setting ?? new ConsumerSetting
            {
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset
            });
            return this;
        }

        public CommandConsumer Shutdown()
        {
            _consumer.Stop();
            _sendReplyService.Stop();
            return this;
        }

        public CommandConsumer Start()
        {
            _sendReplyService.Start();
            _consumer.SetMessageHandler(this).Start();
            return this;
        }

        public CommandConsumer Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return this;
        }

        private class CommandExecuteContext : ICommandExecuteContext
        {
            private readonly IAggregateStorage _aggregateRootStorage;
            private readonly CommandMessage _commandMessage;
            private readonly IMessageContext _messageContext;
            private readonly QueueMessage _queueMessage;
            private readonly IRepository _repository;
            private readonly SendReplyService _sendReplyService;
            private readonly ConcurrentDictionary<string, IAggregateRoot> _trackingAggregateRootDict;
            private string _result;

            public CommandExecuteContext(IRepository repository, IAggregateStorage aggregateRootStorage, QueueMessage queueMessage, IMessageContext messageContext, CommandMessage commandMessage, SendReplyService sendReplyService)
            {
                _trackingAggregateRootDict = new ConcurrentDictionary<string, IAggregateRoot>();
                _repository = repository;
                _aggregateRootStorage = aggregateRootStorage;
                _sendReplyService = sendReplyService;
                _queueMessage = queueMessage;
                _commandMessage = commandMessage;
                _messageContext = messageContext;
            }

            public void Add(IAggregateRoot aggregateRoot)
            {
                if (aggregateRoot == null)
                {
                    throw new ArgumentNullException("aggregateRoot");
                }
                if (!_trackingAggregateRootDict.TryAdd(aggregateRoot.UniqueId, aggregateRoot))
                {
                    throw new AggregateRootAlreadyExistException(aggregateRoot.UniqueId, aggregateRoot.GetType());
                }
            }

            public Task AddAsync(IAggregateRoot aggregateRoot)
            {
                Add(aggregateRoot);
                return Task.CompletedTask;
            }

            public void Clear()
            {
                _trackingAggregateRootDict.Clear();
                _result = null;
            }

            public async Task<T> GetAsync<T>(object id, bool firstFromCache = true) where T : class, IAggregateRoot
            {
                if (id == null)
                {
                    throw new ArgumentNullException("id");
                }

                var aggregateRootId = id.ToString();
                if (_trackingAggregateRootDict.TryGetValue(aggregateRootId, out IAggregateRoot aggregateRoot))
                {
                    return aggregateRoot as T;
                }

                if (firstFromCache)
                {
                    aggregateRoot = await _repository.GetAsync<T>(id);
                }
                else
                {
                    aggregateRoot = await _aggregateRootStorage.GetAsync(typeof(T), aggregateRootId);
                }

                if (aggregateRoot != null)
                {
                    _trackingAggregateRootDict.TryAdd(aggregateRoot.UniqueId, aggregateRoot);
                    return aggregateRoot as T;
                }

                return null;
            }

            public string GetResult()
            {
                return _result;
            }

            public IEnumerable<IAggregateRoot> GetTrackedAggregateRoots()
            {
                return _trackingAggregateRootDict.Values;
            }

            public Task OnCommandExecutedAsync(CommandResult commandResult)
            {
                _messageContext.OnMessageHandled(_queueMessage);

                if (string.IsNullOrEmpty(_commandMessage.ReplyAddress))
                {
                    return Task.CompletedTask;
                }

                return _sendReplyService.SendReply((int)CommandReturnType.CommandExecuted, commandResult, _commandMessage.ReplyAddress);
            }

            public void SetResult(string result)
            {
                _result = result;
            }
        }
    }
}