using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using ENode.Kafka.Producers;
using ENode.Messaging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class ApplicationMessagePublisher : IMessagePublisher<IApplicationMessage>
    {
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private ITopicProvider<IApplicationMessage> _messageTopicProvider;
        private SendQueueMessageService _sendMessageService;
        private ITypeNameProvider _typeNameProvider;
        public Producer Producer { get; private set; }

        public ApplicationMessagePublisher InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageTopicProvider = ObjectContainer.Resolve<ITopicProvider<IApplicationMessage>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _sendMessageService = new SendQueueMessageService();
            return this;
        }

        public ApplicationMessagePublisher InitializeKafka(ProducerSetting producerSetting, Dictionary<string, object> kafkaConfig = null)
        {
            InitializeENode();
            Producer = new Producer(producerSetting);
            return this;
        }

        public Task PublishAsync(IApplicationMessage message)
        {
            var enodeMessage = CreateENodeMessage(message);
            return _sendMessageService.SendMessageAsync(Producer, "applicationMessage", message.GetType().Name, enodeMessage, message.Id, message.Id, message.Items);
        }

        public ApplicationMessagePublisher Shutdown()
        {
            Producer.Stop();
            return this;
        }

        public ApplicationMessagePublisher Start()
        {
            Producer.OnLog = (_, info) => _logger.Info($"ENode ApplicationMessagePublisher: {info}");
            Producer.OnError = (_, error) => _logger.Error($"ENode ApplicationMessagePublisher has an error: {error}");
            Producer.Start();

            return this;
        }

        private ENodeMessage CreateENodeMessage(IApplicationMessage message)
        {
            var topic = _messageTopicProvider.GetTopic(message);
            var data = _jsonSerializer.Serialize(message);
            return new ENodeMessage(
                topic,
                (int)ENodeMessageTypeCode.ApplicationMessage,
                data,
                _typeNameProvider.GetTypeName(message.GetType()));
        }
    }
}