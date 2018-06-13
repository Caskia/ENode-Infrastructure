using Confluent.Kafka;
using ECommon.Logging;
using ECommon.Scheduling;
using System.Collections.Concurrent;
using System;
using ECommon.Components;
using ENode.Kafka.Extensions;
using System.Threading.Tasks;

namespace ENode.Kafka.Consumers
{
    public class ConsumingMessageService
    {
        #region Private Variables

        private readonly Worker _consumeMessageWorker;
        private readonly BlockingCollection<Message<Ignore, string>> _consumeWaitingQueue;
        private readonly ConcurrentDictionary<string, TopicPartitionProcessQueue<Ignore, string>> _consumingQueues;
        private readonly ILogger _logger;
        private readonly BlockingCollection<Message<Ignore, string>> _retryQueue;
        private readonly IScheduleService _scheduleService;
        private Consumer _consumer;
        private IMessageHandler<Ignore, string> _messageHandler;

        private bool _isSequentialConsume
        {
            get
            {
                if (_consumer == null)
                {
                    throw new ArgumentNullException(nameof(_consumer));
                }
                return _consumer.Setting.MessageHandleMode == MessageHandleMode.Sequential;
            }
        }

        #endregion Private Variables

        public ConsumingMessageService(Consumer consumer)
        {
            _consumer = consumer;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();

            if (_isSequentialConsume)
            {
                _consumeWaitingQueue = new BlockingCollection<Message<Ignore, string>>();
                _consumeMessageWorker = new Worker("ConsumeMessage", () => HandleMessage(_consumeWaitingQueue.Take()));
            }
            _consumingQueues = new ConcurrentDictionary<string, TopicPartitionProcessQueue<Ignore, string>>();
            _retryQueue = new BlockingCollection<Message<Ignore, string>>();
        }

        #region Public Methods

        public void EnterConsumingQueue(Message<Ignore, string> message)
        {
            if (!_consumingQueues.TryGetValue(message.ToKeyString(), out var processQueue))
            {
                processQueue.AddMessage(message);
            }
            else
            {
                var queue = new TopicPartitionProcessQueue<Ignore, string>(message.Topic, message.Partition);
                queue.AddMessage(message);
                _consumingQueues.TryAdd(message.ToKeyString(), queue);
            }

            if (_isSequentialConsume)
            {
                _consumeWaitingQueue.Add(message);
            }
            else
            {
                Task.Factory.StartNew(HandleMessage, message);
            }
        }

        public void SetMessageHandler(IMessageHandler<Ignore, string> messageHandler)
        {
            _messageHandler = messageHandler ?? throw new ArgumentNullException(nameof(messageHandler));
        }

        public void Start()
        {
            if (_messageHandler == null)
            {
                throw new Exception("Cannot start as no messageHandler was set, please call SetMessageHandler first.");
            }
            if (_isSequentialConsume)
            {
                _consumeMessageWorker.Start();
            }

            _scheduleService.StartTask("RetryMessage", RetryMessage, 1000, _consumer.Setting.RetryMessageInterval);
            _scheduleService.StartTask("CommitOffsets", async () => { await CommitOffsetsAsync(); }, 1000, _consumer.Setting.CommitConsumerOffsetInterval);

            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            if (_isSequentialConsume)
            {
                _consumeMessageWorker.Stop();
            }

            _scheduleService.StopTask("RetryMessage");
            _scheduleService.StopTask("CommitOffsets");

            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }

        #endregion Public Methods

        #region Private Methods

        private async Task CommitOffsetsAsync()
        {
            foreach (var consumerQueue in _consumingQueues)
            {
                var consumedOffset = consumerQueue.Value.GetConsumedQueueOffset();
                if (consumedOffset >= 0)
                {
                    if (!consumerQueue.Value.TryUpdatePreviousConsumedQueueOffset(consumedOffset))
                    {
                        continue;
                    }
                    await _consumer.CommitConsumeOffsetAsync(consumerQueue.Value.Topic, consumerQueue.Value.Partition, consumedOffset);
                }
            }
        }

        private void HandleMessage(object parameter)
        {
            var message = parameter as Message<Ignore, string>;
            if (_consumer.Stopped) return;
            if (message == null) return;

            try
            {
                _messageHandler.Handle(message, new MessageContext(currentQueueMessage => { RemoveHandledMessage(message); }));
            }
            catch (Exception ex)
            {
                LogMessageHandlingException(message, ex);
                _retryQueue.Add(message);
            }
        }

        private void LogMessageHandlingException(Message<Ignore, string> message, Exception exception)
        {
            _logger.Error(
                $"Message handling has exception, message info:[topic:{message.Topic}, partition:{message.Partition}, partitionOffset:{message.Offset.Value}, createdTime:{message.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}, consumerGroup:{_consumer.Setting.GroupName}]",
                exception);
        }

        private void RemoveHandledMessage(Message<Ignore, string> message)
        {
            if (_consumingQueues.TryGetValue(message.ToKeyString(), out var processQueue))
            {
                processQueue.RemoveMessage(message);
            }
        }

        private void RetryMessage()
        {
            if (_retryQueue.TryTake(out var message))
            {
                HandleMessage(message);
            }
        }

        #endregion Private Methods
    }
}