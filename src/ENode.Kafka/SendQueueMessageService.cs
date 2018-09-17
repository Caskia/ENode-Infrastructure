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
                var result = await producer.ProduceAsync(message.Topic, routingKey, content);
                if (result.Error.HasError)
                {
                    _logger.ErrorFormat("ENode message async send failed, sendResult: {0}, routingKey: {1}, messageId: {2}, version: {3}", result, routingKey, messageId, version);
                    return new AsyncTaskResult(AsyncTaskStatus.IOException, result.Error.Reason);
                }
                _logger.InfoFormat("ENode message async send success, sendResult: {0}, routingKey: {1}, messageId: {2}, version: {3}", result, routingKey, messageId, version);
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