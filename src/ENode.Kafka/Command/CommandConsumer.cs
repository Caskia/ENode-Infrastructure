﻿using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Domain;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using IKafkaMessageHandler = ENode.Kafka.Consumers.IMessageHandler<Confluent.Kafka.Ignore, string>;
using IKafkaMessageContext = ENode.Kafka.Consumers.IMessageContext<Confluent.Kafka.Ignore, string>;
using KafkaMessage = Confluent.Kafka.Message<Confluent.Kafka.Ignore, string>;

namespace ENode.Kafka
{
    public class CommandConsumer : IKafkaMessageHandler
    {
        private const string DefaultCommandConsumerGroup = "CommandConsumerGroup";
        private IAggregateStorage _aggregateStorage;
        private ICommandProcessor _commandProcessor;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IRepository _repository;
        private SendReplyService _sendReplyService;
        private ITypeNameProvider _typeNameProvider;

        public Consumer Consumer { get; private set; }

        void IKafkaMessageHandler.Handle(KafkaMessage message, IKafkaMessageContext context)
        {
            var commandItems = new Dictionary<string, string>();
            var enodeMessage = _jsonSerializer.Deserialize<EnodeMessage>(message.Value);
            var commandMessage = _jsonSerializer.Deserialize<CommandMessage>(Encoding.UTF8.GetString(enodeMessage.Body));
            var commandType = _typeNameProvider.GetType(enodeMessage.Tag);
            var command = _jsonSerializer.Deserialize(commandMessage.CommandData, commandType) as ICommand;
            var commandExecuteContext = new CommandExecuteContext(_repository, _aggregateStorage, message, context, commandMessage, _sendReplyService);
            commandItems["CommandReplyAddress"] = commandMessage.ReplyAddress;
            _logger.InfoFormat("ENode command message received, messageId: {0}, aggregateRootId: {1}", command.Id, command.AggregateRootId);
            _commandProcessor.Process(new ProcessingCommand(command, commandExecuteContext, commandItems));
        }

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

        public CommandConsumer InitializeKafka(ConsumerSetting consumerSetting)
        {
            InitializeENode();

            Consumer = new Consumer(consumerSetting);

            return this;
        }

        public CommandConsumer Shutdown()
        {
            Consumer.Stop();
            _sendReplyService.Stop();
            return this;
        }

        public CommandConsumer Start()
        {
            _sendReplyService.Start();

            Consumer.OnError += (_, error) => _logger.Error($"ENode CommandConsumer has an error: {error}");
            Consumer.OnConsumeError += (_, error) => _logger.Error($"ENode CommandConsumer consume message has an error: {error}");
            Consumer.Start();

            return this;
        }

        public CommandConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }

        public CommandConsumer Subscribe(IList<string> topics)
        {
            Consumer.Subscribe(topics);
            return this;
        }

        private class CommandExecuteContext : ICommandExecuteContext
        {
            private readonly IAggregateStorage _aggregateRootStorage;
            private readonly CommandMessage _commandMessage;
            private readonly KafkaMessage _message;
            private readonly IKafkaMessageContext _messageContext;
            private readonly IRepository _repository;
            private readonly SendReplyService _sendReplyService;
            private readonly ConcurrentDictionary<string, IAggregateRoot> _trackingAggregateRootDict;
            private string _result;

            public CommandExecuteContext(IRepository repository, IAggregateStorage aggregateRootStorage, KafkaMessage message, IKafkaMessageContext messageContext, CommandMessage commandMessage, SendReplyService sendReplyService)
            {
                _trackingAggregateRootDict = new ConcurrentDictionary<string, IAggregateRoot>();
                _repository = repository;
                _aggregateRootStorage = aggregateRootStorage;
                _sendReplyService = sendReplyService;
                _message = message;
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
                _messageContext.OnMessageHandled(_message);

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