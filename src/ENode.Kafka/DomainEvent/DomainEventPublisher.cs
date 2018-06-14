using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Eventing;
using ENode.Infrastructure;
using ENode.Kafka.Producers;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class DomainEventPublisher : IMessagePublisher<DomainEventStreamMessage>
    {
        private IEventSerializer _eventSerializer;
        private ITopicProvider<IDomainEvent> _eventTopicProvider;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private SendQueueMessageService _sendMessageService;

        public Producer Producer { get; private set; }

        public DomainEventPublisher InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _eventTopicProvider = ObjectContainer.Resolve<ITopicProvider<IDomainEvent>>();
            _eventSerializer = ObjectContainer.Resolve<IEventSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendMessageService = new SendQueueMessageService();
            return this;
        }

        public DomainEventPublisher InitializeKafka(ProducerSetting producerSetting)
        {
            InitializeENode();
            Producer = new Producer(producerSetting);
            return this;
        }

        public Task<AsyncTaskResult> PublishAsync(DomainEventStreamMessage eventStream)
        {
            var message = CreateENodeMessage(eventStream);
            return _sendMessageService.SendMessageAsync(Producer, message, eventStream.GetRoutingKey() ?? eventStream.AggregateRootId, eventStream.Id, eventStream.Version.ToString());
        }

        public DomainEventPublisher Shutdown()
        {
            Producer.Stop();
            return this;
        }

        public DomainEventPublisher Start()
        {
            Producer.OnLog = (_, info) => _logger.Info($"ENode DomainEventPublisher: {info}");
            Producer.OnError = (_, error) => _logger.Error($"ENode DomainEventPublisher has an error: {error}");
            Producer.Start();
            return this;
        }

        private ENodeMessage CreateENodeMessage(DomainEventStreamMessage eventStream)
        {
            Ensure.NotNull(eventStream.AggregateRootId, "aggregateRootId");
            var eventMessage = CreateEventMessage(eventStream);
            var topic = _eventTopicProvider.GetTopic(eventStream.Events.First());
            var data = _jsonSerializer.Serialize(eventMessage);
            return new ENodeMessage(topic, (int)ENodeMessageTypeCode.DomainEventStreamMessage, Encoding.UTF8.GetBytes(data));
        }

        private EventStreamMessage CreateEventMessage(DomainEventStreamMessage eventStream)
        {
            var message = new EventStreamMessage();

            message.Id = eventStream.Id;
            message.CommandId = eventStream.CommandId;
            message.AggregateRootTypeName = eventStream.AggregateRootTypeName;
            message.AggregateRootId = eventStream.AggregateRootId;
            message.Timestamp = eventStream.Timestamp;
            message.Version = eventStream.Version;
            message.Events = _eventSerializer.Serialize(eventStream.Events);
            message.Items = eventStream.Items;

            return message;
        }
    }
}