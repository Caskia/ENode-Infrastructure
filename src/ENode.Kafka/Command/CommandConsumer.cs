using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Domain;
using ENode.Infrastructure;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class CommandConsumer
    {
        private const string DefaultCommandConsumerGroup = "CommandConsumerGroup";
        private IAggregateStorage _aggregateStorage;
        private ICommandProcessor _commandProcessor;
        private Consumer<Ignore, string> _consumer;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IRepository _repository;
        private SendReplyService _sendReplyService;
        private ITypeNameProvider _typeNameProvider;
        private bool isStopped = false;
        public Consumer<Ignore, string> Consumer { get { return _consumer; } }

        public CommandConsumer InitializeENode()
        {
            _sendReplyService = new SendReplyService();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _commandProcessor = ObjectContainer.Resolve<ICommandProcessor>();
            _repository = ObjectContainer.Resolve<IRepository>();
            _aggregateStorage = ObjectContainer.Resolve<IAggregateStorage>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }

        public CommandConsumer InitializeKafka(Dictionary<string, object> kafkaConfig = null)
        {
            InitializeENode();

            if (!kafkaConfig.ContainsKey("group.id"))
            {
                kafkaConfig.Add("group.id", DefaultCommandConsumerGroup);
            }
            _consumer = new Consumer<Ignore, string>(kafkaConfig, null, new StringDeserializer(Encoding.UTF8));

            return this;
        }

        public CommandConsumer Shutdown()
        {
            isStopped = true;
            _consumer.Dispose();
            _sendReplyService.Stop();
            return this;
        }

        public CommandConsumer Start()
        {
            _sendReplyService.Start();

            _consumer.OnError += (_, error) => _logger.Error($"ENode CommandConsumer has an error: {error}");

            _consumer.OnConsumeError += (_, error) => _logger.Error($"ENode CommandConsumer consume message has an error: {error}");

            Task.Factory.StartNew(() =>
            {
                while (!isStopped)
                {
                    if (!_consumer.Consume(out var message, TimeSpan.FromMilliseconds(100)))
                    {
                        continue;
                    }

                    HandleMessage(message);
                }
            });

            return this;
        }

        public CommandConsumer Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return this;
        }

        public CommandConsumer Subscribe(IEnumerable<string> topics)
        {
            _consumer.Subscribe(topics);
            return this;
        }

        private void HandleMessage(Message<Ignore, string> message)
        {
            var commandItems = new Dictionary<string, string>();
            var kafkaMessage = _jsonSerializer.Deserialize<KafkaMessage>(message.Value);
            var commandMessage = _jsonSerializer.Deserialize<CommandMessage>(Encoding.UTF8.GetString(kafkaMessage.Body));
            var commandType = _typeNameProvider.GetType(kafkaMessage.Tag);
            var command = _jsonSerializer.Deserialize(commandMessage.CommandData, commandType) as ICommand;
            var commandExecuteContext = new CommandExecuteContext(_repository, _aggregateStorage, message, _consumer, commandMessage, _sendReplyService);
            commandItems["CommandReplyAddress"] = commandMessage.ReplyAddress;
            _logger.InfoFormat("ENode command message received, messageId: {0}, aggregateRootId: {1}", command.Id, command.AggregateRootId);
            _commandProcessor.Process(new ProcessingCommand(command, commandExecuteContext, commandItems));
        }

        private class CommandExecuteContext : ICommandExecuteContext
        {
            private readonly IAggregateStorage _aggregateRootStorage;
            private readonly CommandMessage _commandMessage;
            private readonly Consumer<Ignore, string> _consumer;
            private readonly Message<Ignore, string> _message;
            private readonly IRepository _repository;
            private readonly SendReplyService _sendReplyService;
            private readonly ConcurrentDictionary<string, IAggregateRoot> _trackingAggregateRootDict;
            private string _result;

            public CommandExecuteContext(IRepository repository, IAggregateStorage aggregateRootStorage, Message<Ignore, string> message, Consumer<Ignore, string> consumer, CommandMessage commandMessage, SendReplyService sendReplyService)
            {
                _trackingAggregateRootDict = new ConcurrentDictionary<string, IAggregateRoot>();
                _repository = repository;
                _aggregateRootStorage = aggregateRootStorage;
                _sendReplyService = sendReplyService;
                _message = message;
                _commandMessage = commandMessage;
                _consumer = consumer;
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

            public void Clear()
            {
                _trackingAggregateRootDict.Clear();
                _result = null;
            }

            public T Get<T>(object id, bool firstFromCache = true) where T : class, IAggregateRoot
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
                    aggregateRoot = _repository.Get<T>(id);
                }
                else
                {
                    aggregateRoot = _aggregateRootStorage.Get(typeof(T), aggregateRootId);
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

            public void OnCommandExecuted(CommandResult commandResult)
            {
                _consumer.CommitAsync(_message);

                if (string.IsNullOrEmpty(_commandMessage.ReplyAddress))
                {
                    return;
                }

                _sendReplyService.SendReply((int)CommandReturnType.CommandExecuted, commandResult, _commandMessage.ReplyAddress);
            }

            public void SetResult(string result)
            {
                _result = result;
            }
        }
    }
}