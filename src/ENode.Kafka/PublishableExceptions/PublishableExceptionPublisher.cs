using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using ENode.Kafka.Producers;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class PublishableExceptionPublisher : IMessagePublisher<IPublishableException>
    {
        private ITopicProvider<IPublishableException> _exceptionTopicProvider;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private SendQueueMessageService _sendMessageService;
        private ITypeNameProvider _typeNameProvider;
        public Producer Producer { get; private set; }

        public PublishableExceptionPublisher InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _exceptionTopicProvider = ObjectContainer.Resolve<ITopicProvider<IPublishableException>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _sendMessageService = new SendQueueMessageService();
            return this;
        }

        public PublishableExceptionPublisher InitializeKafka(ProducerSetting producerSetting)
        {
            InitializeENode();
            Producer = new Producer(producerSetting);
            return this;
        }

        public Task<AsyncTaskResult> PublishAsync(IPublishableException exception)
        {
            var message = CreateENodeMessage(exception);
            return _sendMessageService.SendMessageAsync(Producer, message, exception.GetRoutingKey() ?? exception.Id, exception.Id, null);
        }

        public PublishableExceptionPublisher Shutdown()
        {
            Producer.Stop();
            return this;
        }

        public PublishableExceptionPublisher Start()
        {
            Producer.OnLog = (_, info) => _logger.Info($"ENode PublishableExceptionPublisher: {info}");
            Producer.OnError = (_, error) => _logger.Error($"ENode PublishableExceptionPublisher has an error: {error}");
            Producer.Start();
            return this;
        }

        private ENodeMessage CreateENodeMessage(IPublishableException exception)
        {
            var topic = _exceptionTopicProvider.GetTopic(exception);
            var serializableInfo = new Dictionary<string, string>();
            exception.SerializeTo(serializableInfo);
            var sequenceMessage = exception as ISequenceMessage;
            var data = _jsonSerializer.Serialize(new PublishableExceptionMessage
            {
                UniqueId = exception.Id,
                AggregateRootTypeName = sequenceMessage != null ? sequenceMessage.AggregateRootTypeName : null,
                AggregateRootId = sequenceMessage != null ? sequenceMessage.AggregateRootStringId : null,
                Timestamp = exception.Timestamp,
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