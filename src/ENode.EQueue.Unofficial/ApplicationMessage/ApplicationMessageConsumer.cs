﻿using System.Text;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Infrastructure;
using EQueue.Clients.Consumers;
using EQueue.Protocols;
using IQueueMessageHandler = EQueue.Clients.Consumers.IMessageHandler;

namespace ENode.EQueue
{
    public class ApplicationMessageConsumer : IQueueMessageHandler
    {
        private const string DefaultMessageConsumerGroup = "ApplicationMessageConsumerGroup";
        private Consumer _consumer;
        private IJsonSerializer _jsonSerializer;
        private ITypeNameProvider _typeNameProvider;
        private IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage> _processor;
        private ILogger _logger;

        public Consumer Consumer { get { return _consumer; } }

        public ApplicationMessageConsumer InitializeENode()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _processor = ObjectContainer.Resolve<IMessageProcessor<ProcessingApplicationMessage, IApplicationMessage>>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            return this;
        }
        public ApplicationMessageConsumer InitializeEQueue(string groupName = null, ConsumerSetting setting = null)
        {
            InitializeENode();
            _consumer = new Consumer(groupName ?? DefaultMessageConsumerGroup, setting ?? new ConsumerSetting
            {
                MessageHandleMode = MessageHandleMode.Sequential,
                ConsumeFromWhere = ConsumeFromWhere.FirstOffset
            });
            return this;
        }

        public ApplicationMessageConsumer Start()
        {
            _consumer.SetMessageHandler(this).Start();
            return this;
        }
        public ApplicationMessageConsumer Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return this;
        }
        public ApplicationMessageConsumer Shutdown()
        {
            _consumer.Stop();
            return this;
        }

        void IQueueMessageHandler.Handle(QueueMessage queueMessage, IMessageContext context)
        {
            var applicationMessageType = _typeNameProvider.GetType(queueMessage.Tag);
            var message = _jsonSerializer.Deserialize(Encoding.UTF8.GetString(queueMessage.Body), applicationMessageType) as IApplicationMessage;
            var processContext = new EQueueProcessContext(queueMessage, context);
            var processingMessage = new ProcessingApplicationMessage(message, processContext);
            _logger.InfoFormat("ENode application message received, messageId: {0}, routingKey: {1}", message.Id, message.GetRoutingKey());
            _processor.Process(processingMessage);
        }
    }
}
