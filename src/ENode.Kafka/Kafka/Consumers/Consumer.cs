using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Scheduling;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ENode.Kafka.Consumers
{
    public class Consumer
    {
        #region Private Variables

        private readonly ConsumingMessageService _consumingMessageService;
        private readonly ILogger _logger;
        private readonly Worker _pollingMessageWorker;
        private readonly IScheduleService _scheduleService;
        private Consumer<Ignore, string> _kafkaConsumer;
        private IMessageHandler<Ignore, string> _messageHandler;

        #endregion Private Variables

        #region Public Variables

        public Action<object, Message> OnConsumeError;
        public Action<object, Error> OnError;
        public Action<object, LogMessage> OnLog;

        #endregion Public Variables

        #region Public Properties

        public ConsumerSetting Setting { get; private set; }

        public bool Stopped { get; private set; }

        #endregion Public Properties

        public Consumer(ConsumerSetting setting)
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();

            InitializeKafkaConsumer(setting);

            _pollingMessageWorker = new Worker("PollingMessage", () => _kafkaConsumer.Poll(TimeSpan.FromMilliseconds(100)));

            _consumingMessageService = new ConsumingMessageService(this);
        }

        #region Public Methods

        public Consumer SetMessageHandler(IMessageHandler<Ignore, string> messageHandler)
        {
            _consumingMessageService.SetMessageHandler(messageHandler);
            return this;
        }

        public void Start()
        {
            RegisterKafkaConsumerEvent();

            _pollingMessageWorker.Start();

            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            Stopped = true;
            _kafkaConsumer.Dispose();

            _pollingMessageWorker.Stop();

            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }

        public Consumer Subscribe(string topic)
        {
            _kafkaConsumer.Subscribe(topic);
            return this;
        }

        public Consumer Subscribe(IList<string> topics)
        {
            _kafkaConsumer.Subscribe(topics);
            return this;
        }

        #endregion Public Methods

        #region Private Methods

        private void InitializeKafkaConsumer(ConsumerSetting setting)
        {
            Setting = setting;

            var kafkaConfig = new Dictionary<string, object>()
            {
                { "enable.auto.commit", false },
                { "session.timeout.ms", 6000 },
                { "auto.offset.reset", "smallest" }
            };

            if (!string.IsNullOrEmpty(setting.GroupName))
            {
                kafkaConfig.Add("group.id", setting.GroupName);
            }

            kafkaConfig.Add("bootstrap.servers", string.Join(",", setting.BrokerEndPoints.Select(e => e.Address.ToString() + ":" + e.Port)));

            _kafkaConsumer = new Consumer<Ignore, string>(kafkaConfig, null, new StringDeserializer(Encoding.UTF8));
        }

        private void RegisterKafkaConsumerEvent()
        {
            if (OnError != null)
            {
                _kafkaConsumer.OnError += (sender, error) => { OnError(sender, error); };
            }

            if (OnConsumeError != null)
            {
                _kafkaConsumer.OnConsumeError += (sender, message) => { OnConsumeError(sender, message); };
            }

            if (OnLog != null)
            {
                _kafkaConsumer.OnLog += (sender, message) => { OnLog(sender, message); };
            }

            _kafkaConsumer.OnMessage += (sender, message) => { _consumingMessageService.EnterConsumingQueue(message); };
        }

        #endregion Private Methods
    }
}