using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using System.Text;
using IKafkaMessageContext = ENode.Kafka.Consumers.IMessageContext<Confluent.Kafka.Ignore, string>;
using IKafkaMessageHandler = ENode.Kafka.Consumers.IMessageHandler<Confluent.Kafka.Ignore, string>;
using KafkaMessage = Confluent.Kafka.ConsumeResult<Confluent.Kafka.Ignore, string>;

namespace ENode.Kafka
{
    public class ApplicationMessageConsumer : IKafkaMessageHandler
    {
        private const string DefaultMessageConsumerGroup = "ApplicationMessageConsumerGroup";
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage> _processor;
        private ITypeNameProvider _typeNameProvider;

        public Consumer Consumer { get; private set; }

        void IKafkaMessageHandler.Handle(KafkaMessage message, IKafkaMessageContext context)
        {
            var eNodeMessage = _jsonSerializer.Deserialize<ENodeMessage>(message.Value);
            var applicationMessageType = _typeNameProvider.GetType(eNodeMessage.Tag);
            var applicationMessage = _jsonSerializer.Deserialize(Encoding.UTF8.GetString(eNodeMessage.Body), applicationMessageType) as IApplicationMessage;
            var processContext = new KafkaMessageProcessContext(message, context);
            var processingMessage = new ProcessingApplicationMessage(applicationMessage, processContext);
            _logger.InfoFormat("ENode application message received, messageId: {0}, routingKey: {1}", applicationMessage.Id, applicationMessage.GetRoutingKey());
            _processor.Process(processingMessage);
        }

        public ApplicationMessageConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _processor = ObjectContainer.Resolve<IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }

        public ApplicationMessageConsumer InitializeKafka(ConsumerSetting consumerSetting)
        {
            InitializeENode();

            Consumer = new Consumer(consumerSetting);

            return this;
        }

        public ApplicationMessageConsumer Shutdown()
        {
            Consumer.Stop();
            return this;
        }

        public ApplicationMessageConsumer Start()
        {
            Consumer.OnError = (_, error) => _logger.Error($"ENode ApplicationMessageConsumer has an error: {error}");
            Consumer.SetMessageHandler(this).Start();

            return this;
        }

        public ApplicationMessageConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }
    }
}