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
        private Producer<string, string> _kafkaProducer;

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

        public Task<DeliveryReport<string, string>> ProduceAsync(string topic, string routingKey, string content)
        {
            return _kafkaProducer.ProduceAsync(topic, new Message<string, string>() { Key = routingKey, Value = content });
        }

        public void Start()
        {
            RegisterKafkaProducerEvent();

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
                BootstrapServers = string.Join(",", setting.BrokerEndPoints.Select(e => e.Address.ToString() + ":" + e.Port))
            };

            _kafkaProducer = new Producer<string, string>(kafkaConfig);
        }

        private void RegisterKafkaProducerEvent()
        {
            if (OnError != null)
            {
                _kafkaProducer.OnError += (sender, error) => { OnError(sender, error); };
            }

            if (OnLog != null)
            {
                _kafkaProducer.OnLog += (sender, message) => { OnLog(sender, message); };
            }
        }

        #endregion Private Methods
    }
}