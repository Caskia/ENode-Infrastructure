using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Kafka.Producers;
using System;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    internal class SendQueueMessageService
    {
        private readonly IOHelper _ioHelper;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        public SendQueueMessageService()
        {
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public async Task<AsyncTaskResult> SendMessageAsync(Producer producer, ENodeMessage message, string routingKey, string messageId, string version)
        {
            try
            {
                var content = _jsonSerializer.Serialize(message);
                await producer.ProduceAsync(message.Topic, routingKey, content);
                _logger.InfoFormat("ENode message async send success, routingKey: {0}, messageId: {1}, version: {2}", routingKey, messageId, version);
                return AsyncTaskResult.Success;
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("ENode message async send has exception, message: {0}, routingKey: {1}, messageId: {2}, version: {3}", message, routingKey, messageId, version), ex);
                return new AsyncTaskResult(AsyncTaskStatus.IOException, ex.Message);
            }
        }
    }
}