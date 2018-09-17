using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Commanding;
using ENode.Eventing;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using System.Collections.Generic;
using System.Text;
using IKafkaMessageContext = ENode.Kafka.Consumers.IMessageContext<Confluent.Kafka.Ignore, string>;
using IKafkaMessageHandler = ENode.Kafka.Consumers.IMessageHandler<Confluent.Kafka.Ignore, string>;
using KafkaMessage = Confluent.Kafka.Message<Confluent.Kafka.Ignore, string>;

namespace ENode.Kafka
{
    public class DomainEventConsumer : IKafkaMessageHandler
    {
        private const string DefaultEventConsumerGroup = "EventConsumerGroup";
        private IEventSerializer _eventSerializer;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IMessageProcessor<ProcessingDomainEventStreamMessage, DomainEventStreamMessage> _messageProcessor;
        private bool _sendEventHandledMessage;
        private SendReplyService _sendReplyService;

        public Consumer Consumer { get; private set; }

        public void Handle(KafkaMessage message, IKafkaMessageContext context)
        {
            var eNodeMessage = _jsonSerializer.Deserialize<ENodeMessage>(message.Value);
            var eventStreamMessage = _jsonSerializer.Deserialize<EventStreamMessage>(Encoding.UTF8.GetString(eNodeMessage.Body));
            var domainEventStreamMessage = ConvertToDomainEventStream(eventStreamMessage);
            var processContext = new DomainEventStreamProcessContext(this, domainEventStreamMessage, message, context);
            var processingMessage = new ProcessingDomainEventStreamMessage(domainEventStreamMessage, processContext);
            _logger.InfoFormat("ENode event message received, messageId: {0}, aggregateRootId: {1}, aggregateRootType: {2}, version: {3}", domainEventStreamMessage.Id, domainEventStreamMessage.AggregateRootStringId, domainEventStreamMessage.AggregateRootTypeName, domainEventStreamMessage.Version);
            _messageProcessor.Process(processingMessage);
        }

        public DomainEventConsumer InitializeENode(bool sendEventHandledMessage = true)
        {
            _sendReplyService = new SendReplyService("EventConsumerSendReplyService");
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _eventSerializer = ObjectContainer.Resolve<IEventSerializer>();
            _messageProcessor = ObjectContainer.Resolve<IMessageProcessor<ProcessingDomainEventStreamMessage, DomainEventStreamMessage>>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendEventHandledMessage = sendEventHandledMessage;
            return this;
        }

        public DomainEventConsumer InitializeKafka(ConsumerSetting consumerSetting, bool sendEventHandledMessage = true)
        {
            InitializeENode(sendEventHandledMessage);

            Consumer = new Consumer(consumerSetting);

            return this;
        }

        public DomainEventConsumer Shutdown()
        {
            Consumer.Stop();
            if (_sendEventHandledMessage)
            {
                _sendReplyService.Stop();
            }
            return this;
        }

        public DomainEventConsumer Start()
        {
            _sendReplyService.Start();

            Consumer.OnError += (_, error) => _logger.Error($"ENode DomainEventConsumer has an error: {error}");
            Consumer.OnConsumeError += (_, error) => _logger.Error($"ENode DomainEventConsumer consume message has an error: {error}");
            Consumer.SetMessageHandler(this).Start();

            return this;
        }

        public DomainEventConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }

        public DomainEventConsumer Subscribe(IList<string> topics)
        {
            Consumer.Subscribe(topics);
            return this;
        }

        private DomainEventStreamMessage ConvertToDomainEventStream(EventStreamMessage message)
        {
            var domainEventStreamMessage = new DomainEventStreamMessage(
                message.CommandId,
                message.AggregateRootId,
                message.Version,
                message.AggregateRootTypeName,
                _eventSerializer.Deserialize<IDomainEvent>(message.Events),
                message.Items)
            {
                Id = message.Id,
                Timestamp = message.Timestamp
            };
            return domainEventStreamMessage;
        }

        private class DomainEventStreamProcessContext : KafkaMessageProcessContext
        {
            private readonly DomainEventStreamMessage _domainEventStreamMessage;
            private readonly DomainEventConsumer _eventConsumer;

            public DomainEventStreamProcessContext(DomainEventConsumer eventConsumer, DomainEventStreamMessage domainEventStreamMessage, KafkaMessage message, IKafkaMessageContext messageContext)
                : base(message, messageContext)
            {
                _eventConsumer = eventConsumer;
                _domainEventStreamMessage = domainEventStreamMessage;
            }

            public override void NotifyMessageProcessed()
            {
                base.NotifyMessageProcessed();

                if (!_eventConsumer._sendEventHandledMessage)
                {
                    return;
                }

                if (!_domainEventStreamMessage.Items.TryGetValue("CommandReplyAddress", out string replyAddress) || string.IsNullOrEmpty(replyAddress))
                {
                    return;
                }
                _domainEventStreamMessage.Items.TryGetValue("CommandResult", out string commandResult);

                _eventConsumer._sendReplyService.SendReply((int)CommandReturnType.EventHandled, new DomainEventHandledMessage
                {
                    CommandId = _domainEventStreamMessage.CommandId,
                    AggregateRootId = _domainEventStreamMessage.AggregateRootId,
                    CommandResult = commandResult
                }, replyAddress);
            }
        }
    }
}