using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using ECommon.Components;
using ECommon.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

        public async Task<Message<string, string>> ProduceAsync(string topic, string routingKey, string content)
        {
            return await _kafkaProducer.ProduceAsync(topic, routingKey, content);
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

            var kafkaConfig = new Dictionary<string, object>()
            {
                { "bootstrap.servers",string.Join(",", setting.BrokerEndPoints.Select(e => e.Address.ToString() + ":" + e.Port))}
            };

            _kafkaProducer = new Producer<string, string>(kafkaConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8));
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