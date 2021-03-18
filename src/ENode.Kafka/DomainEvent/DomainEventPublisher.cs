using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Eventing;
using ENode.Kafka.Producers;
using ENode.Messaging;
using System.Linq;
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
        private TopicsManager _topicsManager;
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

        public DomainEventPublisher InitializeKafka(ProducerSetting setting)
        {
            InitializeENode();

            Producer = new Producer(setting);

            _topicsManager = new TopicsManager(setting.BootstrapServers);
            return this;
        }

        public Task PublishAsync(DomainEventStreamMessage eventStream)
        {
            var message = CreateENodeMessage(eventStream);
            return _sendMessageService.SendMessageAsync(Producer, "events", string.Join(",", eventStream.Events.Select(x => x.GetType().Name)), message, eventStream.AggregateRootId, eventStream.Id, eventStream.Items);
        }

        public DomainEventPublisher Shutdown()
        {
            Producer.Stop();
            return this;
        }

        public DomainEventPublisher Start()
        {
            //create topic
            _topicsManager.CheckAndCreateTopicsAsync(_eventTopicProvider.GetAllTopics()).GetAwaiter().GetResult();

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
            return new ENodeMessage(topic, (int)ENodeMessageTypeCode.DomainEventStreamMessage, data);
        }

        private EventStreamMessage CreateEventMessage(DomainEventStreamMessage eventStream)
        {
            var message = new EventStreamMessage()
            {
                Id = eventStream.Id,
                CommandId = eventStream.CommandId,
                AggregateRootTypeName = eventStream.AggregateRootTypeName,
                AggregateRootId = eventStream.AggregateRootId,
                Timestamp = eventStream.Timestamp,
                Version = eventStream.Version,
                Events = _eventSerializer.Serialize(eventStream.Events),
                Items = eventStream.Items
            };

            return message;
        }
    }
}