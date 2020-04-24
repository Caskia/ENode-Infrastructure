using Confluent.Kafka;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Utilities;
using ENode.Kafka.Extensions;
using ENode.Kafka.Scheduling;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace ENode.Kafka.Consumers
{
    public class ConsumingMessageService
    {
        #region Private Variables

        private readonly AsyncWorker _consumeMessageWorker;
        private readonly BlockingCollection<ConsumeResult<Ignore, string>> _consumeWaitingQueue;
        private readonly ConcurrentDictionary<string, TopicPartitionProcessQueue<Ignore, string>> _consumingQueues;
        private readonly ILogger _logger;
        private readonly BlockingCollection<ConsumeResult<Ignore, string>> _retryQueue;
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
                _consumeWaitingQueue = new BlockingCollection<ConsumeResult<Ignore, string>>();
                _consumeMessageWorker = new AsyncWorker("ConsumeMessage", async () => await HandleMessageAsync(_consumeWaitingQueue.Take()));
            }
            _consumingQueues = new ConcurrentDictionary<string, TopicPartitionProcessQueue<Ignore, string>>();
            _retryQueue = new BlockingCollection<ConsumeResult<Ignore, string>>();
        }

        #region Public Methods

        public void EnterConsumingQueue(ConsumeResult<Ignore, string> message)
        {
            TopicPartitionProcessQueue<Ignore, string> processQueue;

            if (_consumingQueues.TryGetValue(message.ToKeyString(), out processQueue))
            {
                processQueue.AddMessage(message);
            }
            else
            {
                processQueue = new TopicPartitionProcessQueue<Ignore, string>(message.Topic, message.Partition);
                processQueue.AddMessage(message);
                _consumingQueues.TryAdd(message.ToKeyString(), processQueue);
            }

            if (_isSequentialConsume)
            {
                _consumeWaitingQueue.Add(message);
            }
            else
            {
                Task.Factory.StartNew(async m =>
                {
                    await HandleMessageAsync(m);
                }, message);
            }

            var unconsumedMessageCount = processQueue.GetMessageCount();

            if (unconsumedMessageCount > _consumer.Setting.ConsumeFlowControlThreshold)
            {
                var delayMilliseconds = FlowControlUtil.CalculateFlowControlTimeMilliseconds
                (
                    unconsumedMessageCount,
                    _consumer.Setting.ConsumeFlowControlThreshold,
                    _consumer.Setting.ConsumeFlowControlStepPercent,
                    _consumer.Setting.ConsumeFlowControlStepWaitMilliseconds
                );

                _logger.InfoFormat($"{message.ToKeyString()} unconsumed message[{unconsumedMessageCount}], consume too slow, need to wait {delayMilliseconds}ms.");
                Thread.Sleep(delayMilliseconds);
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
                Task.Run(async () => await _consumeMessageWorker.StartAsync());
            }

            _scheduleService.StartTask("RetryMessage", async () => { await RetryMessageAsync(); }, 1000, _consumer.Setting.RetryMessageInterval);
            _scheduleService.StartTask("CommitOffsets", async () => { await CommitOffsetsAsync(); }, 1000, _consumer.Setting.CommitConsumerOffsetInterval);

            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            if (_isSequentialConsume)
            {
                Task.Run(async () => await _consumeMessageWorker.StopAsync());
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
                    await _consumer.CommitConsumeOffsetAsync(consumerQueue.Value.Topic, consumerQueue.Value.Partition, consumedOffset + 1);
                }
            }
        }

        private async Task HandleMessageAsync(object parameter)
        {
            var message = parameter as ConsumeResult<Ignore, string>;
            if (_consumer.Stopped) return;
            if (message == null) return;

            try
            {
                await _messageHandler.HandleAsync(message, new MessageContext(currentQueueMessage => { RemoveHandledMessage(message); }));
            }
            catch (Exception ex)
            {
                LogMessageHandlingException(message, ex);
                _retryQueue.Add(message);
            }
        }

        private void LogMessageHandlingException(ConsumeResult<Ignore, string> message, Exception exception)
        {
            _logger.Error(
                $"Message handling has exception, message info:[topic:{message.Topic}, partition:{message.Partition}, partitionOffset:{message.Offset.Value}, createdTime:{message.Timestamp.UtcDateTime.ToString("yyyy-MM-dd HH:mm:ss")}, consumerGroup:{_consumer.Setting.GroupName}]",
                exception);
        }

        private void RemoveHandledMessage(ConsumeResult<Ignore, string> message)
        {
            if (_consumingQueues.TryGetValue(message.ToKeyString(), out var processQueue))
            {
                processQueue.RemoveMessage(message);
            }
        }

        private async Task RetryMessageAsync()
        {
            if (_retryQueue.TryTake(out var message))
            {
                await HandleMessageAsync(message);
            }
        }

        #endregion Private Methods
    }
}