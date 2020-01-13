using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text;
using IKafkaMessageContext = ENode.Kafka.Consumers.IMessageContext<Confluent.Kafka.Ignore, string>;
using IKafkaMessageHandler = ENode.Kafka.Consumers.IMessageHandler<Confluent.Kafka.Ignore, string>;
using KafkaMessage = Confluent.Kafka.ConsumeResult<Confluent.Kafka.Ignore, string>;

namespace ENode.Kafka
{
    public class PublishableExceptionConsumer : IKafkaMessageHandler
    {
        private const string DefaultExceptionConsumerGroup = "ExceptionConsumerGroup";
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IMessageProcessor<ProcessingPublishableExceptionMessage, IPublishableException> _publishableExceptionProcessor;
        private ITypeNameProvider _typeNameProvider;
        public Consumer Consumer { get; private set; }

        public void Handle(KafkaMessage message, IKafkaMessageContext context)
        {
            var eNodeMessage = _jsonSerializer.Deserialize<ENodeMessage>(message.Value);
            var exceptionMessage = _jsonSerializer.Deserialize<PublishableExceptionMessage>(eNodeMessage.Body);
            var exceptionType = _typeNameProvider.GetType(eNodeMessage.Tag);
            var exception = FormatterServices.GetUninitializedObject(exceptionType) as IPublishableException;
            exception.Id = exceptionMessage.UniqueId;
            exception.Timestamp = exceptionMessage.Timestamp;
            exception.RestoreFrom(exceptionMessage.SerializableInfo);
            var sequenceMessage = exception as ISequenceMessage;
            if (sequenceMessage != null)
            {
                sequenceMessage.AggregateRootTypeName = exceptionMessage.AggregateRootTypeName;
                sequenceMessage.AggregateRootStringId = exceptionMessage.AggregateRootId;
            }
            var processContext = new KafkaMessageProcessContext(message, context);
            var processingMessage = new ProcessingPublishableExceptionMessage(exception, processContext);
            _logger.InfoFormat("ENode exception message received, messageId: {0}, aggregateRootId: {1}, aggregateRootType: {2}", exceptionMessage.UniqueId, exceptionMessage.AggregateRootId, exceptionMessage.AggregateRootTypeName);
            _publishableExceptionProcessor.Process(processingMessage);
        }

        public PublishableExceptionConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _publishableExceptionProcessor = ObjectContainer.Resolve<IMessageProcessor<ProcessingPublishableExceptionMessage, IPublishableException>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(nameof(PublishableExceptionConsumer));
            return this;
        }

        public PublishableExceptionConsumer InitializeKafka(ConsumerSetting consumerSetting)
        {
            InitializeENode();

            Consumer = new Consumer(consumerSetting);

            return this;
        }

        public PublishableExceptionConsumer Shutdown()
        {
            Consumer.Stop();
            return this;
        }

        public PublishableExceptionConsumer Start()
        {
            Consumer.OnLog += (_, info) => _logger.Info(info.Message);
            Consumer.OnError += (_, error) => _logger.Error($"consumer has an error: {error}");
            Consumer.SetMessageHandler(this).Start();
            return this;
        }

        public PublishableExceptionConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }

        public PublishableExceptionConsumer Subscribe(IList<string> topics)
        {
            Consumer.Subscribe(topics);
            return this;
        }
    }
}