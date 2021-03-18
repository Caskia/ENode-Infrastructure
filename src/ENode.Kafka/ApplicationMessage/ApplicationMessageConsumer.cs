using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using ENode.Messaging;
using System.Collections.Generic;
using System.Threading.Tasks;
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
        private IMessageDispatcher _messageDispatcher;
        private TopicsManager _topicsManager;
        private ITypeNameProvider _typeNameProvider;
        public Consumer Consumer { get; private set; }

        public async Task HandleAsync(KafkaMessage kafkaMessage, IKafkaMessageContext context)
        {
            var eNodeMessage = _jsonSerializer.Deserialize<ENodeMessage>(kafkaMessage.Message.Value);
            var applicationMessageType = _typeNameProvider.GetType(eNodeMessage.Tag);
            var applicationMessage = _jsonSerializer.Deserialize(eNodeMessage.Body, applicationMessageType) as IApplicationMessage;
            _logger.DebugFormat("ENode application message received, messageId: {0}, messageType: {1}", applicationMessage.Id, applicationMessage.GetType().Name);

            await _messageDispatcher.DispatchMessageAsync(applicationMessage).ContinueWith(x =>
            {
                context.OnMessageHandled(kafkaMessage);
            });
        }

        public ApplicationMessageConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _messageDispatcher = ObjectContainer.Resolve<IMessageDispatcher>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(nameof(ApplicationMessageConsumer));
            return this;
        }

        public ApplicationMessageConsumer InitializeKafka(ConsumerSetting setting)
        {
            InitializeENode();

            Consumer = new Consumer(setting);

            _topicsManager = new TopicsManager(setting.BootstrapServers);
            return this;
        }

        public ApplicationMessageConsumer Shutdown()
        {
            Consumer.Stop();
            return this;
        }

        public ApplicationMessageConsumer Start()
        {
            //create topic
            _topicsManager.CheckAndCreateTopicsAsync(Consumer.SubscribedTopics).Wait();

            Consumer.OnLog += (_, info) => _logger.Info(info.Message);
            Consumer.OnError = (_, error) => _logger.Error($"consumer has an error: {error}");
            Consumer.SetMessageHandler(this).Start();

            return this;
        }

        public ApplicationMessageConsumer Subscribe(string topic)
        {
            Consumer.Subscribe(topic);
            return this;
        }

        public ApplicationMessageConsumer Subscribe(IList<string> topics)
        {
            Consumer.Subscribe(topics);
            return this;
        }
    }
}