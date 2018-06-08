using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class ApplicationMessageConsumer
    {
        private const string DefaultMessageConsumerGroup = "ApplicationMessageConsumerGroup";
        private Consumer<Ignore, string> _consumer;
        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage> _processor;
        private ITypeNameProvider _typeNameProvider;
        private bool isStopped = false;

        public Consumer<Ignore, string> Consumer { get { return _consumer; } }

        public ApplicationMessageConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _processor = ObjectContainer.Resolve<IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }

        public ApplicationMessageConsumer InitializeKafka(Dictionary<string, object> kafkaConfig = null)
        {
            InitializeENode();

            if (!kafkaConfig.ContainsKey("group.id"))
            {
                kafkaConfig.Add("group.id", DefaultMessageConsumerGroup);
            }
            _consumer = new Consumer<Ignore, string>(kafkaConfig, null, new StringDeserializer(Encoding.UTF8));

            return this;
        }

        public ApplicationMessageConsumer Shutdown()
        {
            isStopped = true;
            _consumer.Dispose();
            return this;
        }

        public ApplicationMessageConsumer Start()
        {
            _consumer.OnError += (_, error) => _logger.Error($"ENode ApplicationMessageConsumer has an error: {error}");

            _consumer.OnConsumeError += (_, error) => _logger.Error($"ENode ApplicationMessageConsumer consume message has an error: {error}");

            Task.Factory.StartNew(() =>
            {
                while (!isStopped)
                {
                    if (!_consumer.Consume(out var message, TimeSpan.FromMilliseconds(100)))
                    {
                        continue;
                    }

                    HandleMessage(message);
                }
            });

            return this;
        }

        public ApplicationMessageConsumer Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return this;
        }

        private void HandleMessage(Message<Ignore, string> message)
        {
            var kafkaMessage = _jsonSerializer.Deserialize<KafkaMessage>(message.Value);
            var applicationMessageType = _typeNameProvider.GetType(kafkaMessage.Tag);
            var applicationMessage = _jsonSerializer.Deserialize(Encoding.UTF8.GetString(kafkaMessage.Body), applicationMessageType) as IApplicationMessage;
            var processContext = new KafkaProcessContext(_consumer, message);
            var processingMessage = new ProcessingApplicationMessage(applicationMessage, processContext);
            _logger.InfoFormat("ENode application message received, messageId: {0}, routingKey: {1}", applicationMessage.Id, applicationMessage.GetRoutingKey());
            _processor.Process(processingMessage);
        }
    }
}