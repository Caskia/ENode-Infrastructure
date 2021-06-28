using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Domain;
using ENode.Infrastructure;
using ENode.Kafka.Producers;
using ENode.Messaging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class DomainExceptionPublisher : IMessagePublisher<IDomainException>
    {
        private ITopicProvider<IDomainException> _exceptionTopicProvider;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private SendQueueMessageService _sendMessageService;
        private TopicsManager _topicsManager;
        private ITypeNameProvider _typeNameProvider;
        public Producer Producer { get; private set; }

        public DomainExceptionPublisher InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _exceptionTopicProvider = ObjectContainer.Resolve<ITopicProvider<IDomainException>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendMessageService = new SendQueueMessageService();
            return this;
        }

        public DomainExceptionPublisher InitializeKafka(ProducerSetting setting)
        {
            InitializeENode();

            Producer = new Producer(setting);

            _topicsManager = new TopicsManager(setting.BootstrapServers);
            return this;
        }

        public Task PublishAsync(IDomainException exception)
        {
            var message = CreateENodeMessage(exception);
            return _sendMessageService.SendMessageAsync(Producer, "exception", exception.GetType().Name, message, exception.Id, exception.Id, exception.Items);
        }

        public DomainExceptionPublisher Shutdown()
        {
            Producer.Stop();
            return this;
        }

        public DomainExceptionPublisher Start()
        {
            //create topic
            _topicsManager.CheckAndCreateTopicsAsync(_exceptionTopicProvider.GetAllTopics()).GetAwaiter().GetResult();

            Producer.OnLog = (_, info) => _logger.Info($"ENode DomainExceptionPublisher: {info}");
            Producer.OnError = (_, error) => _logger.Error($"ENode DomainExceptionPublisher has an error: {error}");
            Producer.Start();
            return this;
        }

        private ENodeMessage CreateENodeMessage(IDomainException exception)
        {
            var topic = _exceptionTopicProvider.GetTopic(exception);
            var serializableInfo = new Dictionary<string, string>();
            exception.SerializeTo(serializableInfo);
            var data = _jsonSerializer.Serialize(new DomainExceptionMessage
            {
                UniqueId = exception.Id,
                Timestamp = exception.Timestamp,
                Items = exception.Items,
                SerializableInfo = serializableInfo
            });
            return new ENodeMessage(
                topic,
                (int)ENodeMessageTypeCode.ExceptionMessage,
                data,
                _typeNameProvider.GetTypeName(exception.GetType()));
        }
    }
}