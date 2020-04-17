using Confluent.Kafka;
using ECommon.Components;
using ECommon.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.Kafka.Producers
{
    public class Producer
    {
        #region Private Variables

        private readonly ILogger _logger;
        private IProducer<string, string> _kafkaProducer;

        #endregion Private Variables

        #region Public Variables

        public Action<object, Error> OnError;
        public Action<object, LogMessage> OnLog;

        #endregion Public Variables

        #region Public Properties

        public ProducerSetting Setting { get; private set; }

        public bool Stopped { get; private set; }

        #endregion Public Properties

        #region Ctor

        public Producer(ProducerSetting setting)
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            InitializeKafkaProducer(setting);
        }

        #endregion Ctor

        #region Public Methods

        public Task<DeliveryResult<string, string>> ProduceAsync(string topic, string routingKey, string content)
        {
            return _kafkaProducer.ProduceAsync(topic, new Message<string, string>() { Key = routingKey, Value = content });
        }

        public void Start()
        {
            _logger.InfoFormat("{0} startted.", GetType().Name);
        }

        public void Stop()
        {
            Stopped = true;
            _kafkaProducer.Dispose();

            _logger.InfoFormat("{0} stopped.", GetType().Name);
        }

        #endregion Public Methods

        #region Private Methods

        private void InitializeKafkaProducer(ProducerSetting setting)
        {
            Setting = setting ?? throw new ArgumentNullException(nameof(setting));

            var kafkaConfig = new ProducerConfig()
            {
                LingerMs = 5,
                BootstrapServers = string.Join(",", setting.BrokerEndPoints.Select(e => e.Address.ToString() + ":" + e.Port))
            };

            _kafkaProducer = new ProducerBuilder<string, string>(kafkaConfig)
                .SetLogHandler((sender, message) => OnLog?.Invoke(sender, message))
                .SetErrorHandler((sender, error) => OnError?.Invoke(sender, error))
                .Build();
        }

        #endregion Private Methods
    }
}