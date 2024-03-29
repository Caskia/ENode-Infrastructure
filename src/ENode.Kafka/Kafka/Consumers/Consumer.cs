﻿using Confluent.Kafka;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ENode.Kafka.Consumers
{
    public class Consumer
    {
        #region Private Variables

        private readonly CancellationToken _cancellationToken;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ConsumingMessageService _consumingMessageService;
        private readonly ILogger _logger;
        private readonly Worker _pollingMessageWorker;
        private readonly IScheduleService _scheduleService;
        private ConsumerConfig _kafkaConfig;
        private IConsumer<Ignore, string> _kafkaConsumer;
        private TopicsManager _topicsManager;

        #endregion Private Variables

        #region Public Variables

        public Action<object, Error> OnError;
        public Action<object, LogMessage> OnLog;

        #endregion Public Variables

        #region Public Properties

        public bool NeedCreateTopicWhenSubcribe
        {
            get
            {
                return !_kafkaConfig.AllowAutoCreateTopics.HasValue || (_kafkaConfig.AllowAutoCreateTopics.HasValue && _kafkaConfig.AllowAutoCreateTopics.Value);
            }
        }

        public ConsumerSetting Setting { get; private set; }

        public bool Stopped { get; private set; }

        public HashSet<string> SubscribedTopics { get; private set; } = new();

        #endregion Public Properties

        #region Ctor

        public Consumer(ConsumerSetting setting)
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _cancellationTokenSource = new CancellationTokenSource();
            _cancellationToken = _cancellationTokenSource.Token;

            _topicsManager = new TopicsManager(setting.BootstrapServers);

            InitializeKafkaConsumer(setting);

            _consumingMessageService = new ConsumingMessageService(this);

            _pollingMessageWorker = new Worker("PollingMessage", () => ConsumeMessage());
        }

        #endregion Ctor

        #region Public Methods

        public Task CommitConsumeOffsetAsync(string topic, int partition, long offset)
        {
            try
            {
                _kafkaConsumer.Commit(new List<TopicPartitionOffset>()
                {
                    new TopicPartitionOffset(topic,partition,offset)
                });
                if (_logger.IsDebugEnabled)
                {
                    _logger.Debug($"CommitConsumeOffset success, consumerGroup:{Setting.GroupName}, topic:{topic}, partition:{partition}, offset:{offset}");
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"CommitConsumeOffset has exception, consumerGroup:{Setting.GroupName}, topic:{topic}, partition:{partition}, offset:{offset}", ex);
            }

            return Task.CompletedTask;
        }

        public Task CommitConsumeOffsetsAsync(List<(string, int, long)> topicPartitionOffset)
        {
            try
            {
                _kafkaConsumer.Commit(topicPartitionOffset.Select(t => new TopicPartitionOffset(t.Item1, t.Item2, t.Item3)));
                if (_logger.IsDebugEnabled)
                {
                    _logger.Debug($"CommitConsumeOffsets success, consumerGroup:{Setting.GroupName}, TopicPartitionOffset:{string.Join(",", topicPartitionOffset.Select(t => $"topic:{t.Item1}, partition:{t.Item2}, offset:{t.Item3}"))}");
                }
            }
            catch (Exception ex)
            {
                _logger.Error($"CommitConsumeOffsets has exception, consumerGroup:{Setting.GroupName}, TopicPartitionOffset:{string.Join(",", topicPartitionOffset.Select(t => $"topic:{t.Item1}, partition:{t.Item2}, offset:{t.Item3}"))}", ex);
            }

            return Task.CompletedTask;
        }

        public Consumer SetMessageHandler(IMessageHandler<Ignore, string> messageHandler)
        {
            _consumingMessageService.SetMessageHandler(messageHandler);
            return this;
        }

        public void Start()
        {
            //create topic
            _topicsManager.CheckAndCreateTopicsAsync(SubscribedTopics).GetAwaiter().GetResult();

            _pollingMessageWorker.Start();
            _consumingMessageService.Start();

            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            Stopped = true;
            _cancellationTokenSource.Cancel();

            _pollingMessageWorker.Stop();
            _consumingMessageService.Stop();

            Thread.Sleep(100);
            _kafkaConsumer.Close();
            _kafkaConsumer.Dispose();

            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }

        public Consumer Subscribe(string topic)
        {
            if (SubscribedTopics.Contains(topic))
            {
                return this;
            }

            if (NeedCreateTopicWhenSubcribe)
            {
                _topicsManager.CheckAndCreateTopicsAsync(new List<string> { topic }).GetAwaiter().GetResult();
            }

            SubscribedTopics.Add(topic);
            _kafkaConsumer.Subscribe(topic);
            return this;
        }

        public Consumer Subscribe(IList<string> topics)
        {
            var needToSubscribedTopic = topics.Where(t => !SubscribedTopics.Contains(t)).ToList();
            if (!needToSubscribedTopic.Any())
            {
                return this;
            }

            if (NeedCreateTopicWhenSubcribe)
            {
                _topicsManager.CheckAndCreateTopicsAsync(needToSubscribedTopic).GetAwaiter().GetResult();
            }

            needToSubscribedTopic.ForEach(t => SubscribedTopics.Add(t));
            _kafkaConsumer.Subscribe(needToSubscribedTopic);
            return this;
        }

        #endregion Public Methods

        #region Private Methods

        private void ConsumeMessage()
        {
            if (!Stopped && !_cancellationToken.IsCancellationRequested && SubscribedTopics.Any())
            {
                var message = _kafkaConsumer.Consume(_cancellationToken);
                _consumingMessageService.EnterConsumingQueue(message);
            }
        }

        private void InitializeKafkaConsumer(ConsumerSetting setting)
        {
            Setting = setting ?? throw new ArgumentNullException(nameof(setting));

            _kafkaConfig = new ConsumerConfig()
            {
                BootstrapServers = setting.BootstrapServers,
                EnableAutoCommit = false,
                SessionTimeoutMs = 20000,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            if (!string.IsNullOrEmpty(setting.GroupName))
            {
                _kafkaConfig.GroupId = setting.GroupName;
            }

            _kafkaConsumer = new ConsumerBuilder<Ignore, string>(_kafkaConfig)
                .SetLogHandler((sender, message) => OnLog?.Invoke(sender, message))
                .SetErrorHandler((sender, error) => OnError?.Invoke(sender, error))
                .Build();
        }

        #endregion Private Methods
    }
}