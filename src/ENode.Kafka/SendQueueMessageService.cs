using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Kafka.Producers;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    internal class SendQueueMessageService
    {
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        public SendQueueMessageService()
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public async Task SendMessageAsync(Producer producer, string messageType, string messageClass, ENodeMessage message, string routingKey, string messageId, IDictionary<string, string> messageExtensionItems)
        {
            try
            {
                var content = _jsonSerializer.Serialize(message);
                await producer.ProduceAsync(message.Topic, routingKey, content).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("ENode {0} message send has exception, message: {1}, routingKey: {2}, messageType: {3}, messageId: {4}, messageExtensionItems: {5}",
                    messageType,
                    message,
                    routingKey,
                    messageClass,
                    messageId,
                    _jsonSerializer.Serialize(messageExtensionItems)
                ), ex);
                throw new IOException("Send equeue message has exception.", ex);
            }
        }
    }
}