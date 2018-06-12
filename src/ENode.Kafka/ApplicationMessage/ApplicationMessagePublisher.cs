using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class ApplicationMessagePublisher : IMessagePublisher<IApplicationMessage>
    {
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private ITopicProvider<IApplicationMessage> _messageTopicProvider;
        private Producer<string, string> _producer;
        private SendQueueMessageService _sendMessageService;
        private ITypeNameProvider _typeNameProvider;
        public Producer<string, string> Producer { get { return _producer; } }

        public ApplicationMessagePublisher InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _messageTopicProvider = ObjectContainer.Resolve<ITopicProvider<IApplicationMessage>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _sendMessageService = new SendQueueMessageService();
            return this;
        }

        public ApplicationMessagePublisher InitializeKafka(Dictionary<string, object> kafkaConfig = null)
        {
            InitializeENode();
            _producer = new Producer<string, string>(kafkaConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));
            return this;
        }

        public Task<AsyncTaskResult> PublishAsync(IApplicationMessage message)
        {
            var queueMessage = CreateKafkaMessage(message);
            return _sendMessageService.SendMessageAsync(_producer, queueMessage, message.GetRoutingKey() ?? message.Id, message.Id, null);
        }

        public ApplicationMessagePublisher Shutdown()
        {
            _producer.Dispose();
            return this;
        }

        public ApplicationMessagePublisher Start()
        {
            _producer.OnLog += (_, info) => _logger.Info($"ENode ApplicationMessagePublisher: {info}");
            _producer.OnError += (_, error) => _logger.Error($"ENode ApplicationMessagePublisher has an error: {error}");

            return this;
        }

        private KafkaMessage CreateKafkaMessage(IApplicationMessage message)
        {
            var topic = _messageTopicProvider.GetTopic(message);
            var data = _jsonSerializer.Serialize(message);
            return new KafkaMessage(
                topic,
                (int)KafkaMessageTypeCode.ApplicationMessage,
                Encoding.UTF8.GetBytes(data),
                _typeNameProvider.GetTypeName(message.GetType()));
        }
    }
}