using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Domain;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using ENode.Messaging;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using IKafkaMessageContext = ENode.Kafka.Consumers.IMessageContext<Confluent.Kafka.Ignore, string>;
using IKafkaMessageHandler = ENode.Kafka.Consumers.IMessageHandler<Confluent.Kafka.Ignore, string>;
using KafkaMessage = Confluent.Kafka.ConsumeResult<Confluent.Kafka.Ignore, string>;

namespace ENode.Kafka
{
    public class DomainExceptionConsumer : IKafkaMessageHandler
    {
        private const string DefaultExceptionConsumerGroup = "ExceptionConsumerGroup";
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IMessageDispatcher _messageDispatcher;
        private TopicsManager _topicsManager;
        private ITypeNameProvider _typeNameProvider;
        public Consumer Consumer { get; private set; }

        public async Task HandleAsync(KafkaMessage kafkaMessage, IKafkaMessageContext context)
        {
            var eNodeMessage = _jsonSerializer.Deserialize<ENodeMessage>(kafkaMessage.Message.Value);
            var exceptionMessage = _jsonSerializer.Deserialize<DomainExceptionMessage>(eNodeMessage.Body);
            var exceptionType = _typeNameProvider.GetType(eNodeMessage.Tag);
            var exception = FormatterServices.GetUninitializedObject(exceptionType) as IDomainException;
            exception.Id = exceptionMessage.UniqueId;
            exception.Timestamp = exceptionMessage.Timestamp;
            exception.Items = exceptionMessage.Items;
            exception.RestoreFrom(exceptionMessage.SerializableInfo);
            _logger.DebugFormat("ENode domain exception message received, messageId: {0}, exceptionType: {1}",
                exceptionMessage.UniqueId,
                exceptionType.Name);

            await _messageDispatcher.DispatchMessageAsync(exception).ContinueWith(x =>
            {
                context.OnMessageHandled(kafkaMessage);
            });
        }

        public DomainExceptionConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _messageDispatcher = ObjectContainer.Resolve<IMessageDispatcher>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(nameof(DomainExceptionConsumer));
            return this;
        }

        public DomainExceptionConsumer InitializeKafka(ConsumerSetting setting)
        {
            InitializeENode();

            Consumer = new Consumer(setting);

            _topicsManager = new TopicsManager(setting.BootstrapServers);
            return this;
        }

        public DomainExceptionConsumer Shutdown()
        {
            Consumer.Stop();
            return this;
        }

        public DomainExceptionConsumer Start()
        {
            //create topic
            _topicsManager.CheckAndCreateTopicsAsync(Consumer.SubscribedTopics).GetAwaiter().GetResult();

            Consumer.OnLog += (_, info) => _logger.Info(info.Message);
            Consumer.OnError += (_, error) => _logger.Error($"consumer has an error: {error}");
            Consumer.SetMessageHandler(this).Start();
            return this;
        }

        public DomainExceptionConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }

        public DomainExceptionConsumer Subscribe(IList<string> topics)
        {
            Consumer.Subscribe(topics);
            return this;
        }
    }
}