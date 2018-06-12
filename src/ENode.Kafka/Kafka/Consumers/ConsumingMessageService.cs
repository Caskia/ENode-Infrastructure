using Confluent.Kafka;
using ECommon.Logging;
using ECommon.Scheduling;
using System.Collections.Concurrent;
using System;
using ECommon.Components;

namespace ENode.Kafka.Consumers
{
    public class ConsumingMessageService<TKey, TValue>
    {
        #region Private Variables

        private readonly ConcurrentDictionary<string, ProcessQueue<TKey, TValue>> _consumingQueue;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, ProcessQueue<TKey, TValue>> _retryQueue;
        private readonly IScheduleService _scheduleService;
        private Consumer<TKey, TValue> _kafkaConsumer;
        private IMessageHandler<TKey, TValue> _messageHandler;

        #endregion Private Variables

        public ConsumingMessageService(Consumer<TKey, TValue> kafkaConsumer)
        {
            _kafkaConsumer = kafkaConsumer;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _consumingQueue = new ConcurrentDictionary<string, ProcessQueue<TKey, TValue>>();
            _retryQueue = new ConcurrentDictionary<string, ProcessQueue<TKey, TValue>>();
        }

        #region Public Methods

        public void EnterConsumingQueue(Message<TKey, TValue> message)
        {
            throw new NotImplementedException();
        }

        public void SetMessageHandler(IMessageHandler<TKey, TValue> messageHandler)
        {
            _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        }

        #endregion Public Methods
    }
}