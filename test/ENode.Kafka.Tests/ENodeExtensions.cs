using ENode.Commanding;
using ENode.Configurations;
using ENode.Eventing;
using ENode.EventStore.MongoDb;
using ENode.Infrastructure;
using ENode.Kafka.Consumers;
using ENode.Kafka.Producers;
using ENode.Kafka.Tests.CommandsAndEvents.Mocks;
using ENode.Kafka.Utils;
using System.Collections.Generic;
using System.Net;
using System.Reflection;

namespace ENode.Kafka.Tests
{
    public static class ENodeExtensions
    {
        private static ApplicationMessageConsumer _applicationMessageConsumer;
        private static ApplicationMessagePublisher _applicationMessagePublisher;
        private static List<IPEndPoint> _brokerEndPoints;
        private static CommandConsumer _commandConsumer;
        private static CommandService _commandService;
        private static DomainEventConsumer _eventConsumer;
        private static DomainEventPublisher _eventPublisher;
        private static bool _isKafkaInitialized;
        private static bool _isKafkaStarted;
        private static PublishableExceptionConsumer _publishableExceptionConsumer;
        private static PublishableExceptionPublisher _publishableExceptionPublisher;

        public static ENodeConfiguration BuildContainer(this ENodeConfiguration enodeConfiguration)
        {
            enodeConfiguration.GetCommonConfiguration().BuildContainer();
            return enodeConfiguration;
        }

        public static ENodeConfiguration InitializeKafka(this ENodeConfiguration enodeConfiguration, List<IPEndPoint> brokerEndPoints)
        {
            if (_isKafkaInitialized)
            {
                return enodeConfiguration;
            }

            _commandService = new CommandService();
            _eventPublisher = new DomainEventPublisher();
            _applicationMessagePublisher = new ApplicationMessagePublisher();
            _publishableExceptionPublisher = new PublishableExceptionPublisher();

            _brokerEndPoints = brokerEndPoints;

            _isKafkaInitialized = true;

            return enodeConfiguration;
        }

        public static ENodeConfiguration StartKafka(this ENodeConfiguration enodeConfiguration)
        {
            if (_isKafkaStarted)
            {
                _commandService.InitializeENode();
                _eventPublisher.InitializeENode();
                _applicationMessagePublisher.InitializeENode();
                _publishableExceptionPublisher.InitializeENode();

                _commandConsumer.InitializeENode();
                _eventConsumer.InitializeENode();
                _applicationMessageConsumer.InitializeENode();
                _publishableExceptionConsumer.InitializeENode();

                return enodeConfiguration;
            }

            var produceSetting = new ProducerSetting()
            {
                BrokerEndPoints = _brokerEndPoints
            };
            _commandService.InitializeKafka(produceSetting, new CommandResultProcessor().Initialize(new IPEndPoint(SocketUtils.GetLocalIPV4(), 9001)));
            _eventPublisher.InitializeKafka(produceSetting);
            _applicationMessagePublisher.InitializeKafka(produceSetting);
            _publishableExceptionPublisher.InitializeKafka(produceSetting);

            var consumerSetting = new ConsumerSetting()
            {
                BrokerEndPoints = _brokerEndPoints,
                CommitConsumerOffsetInterval = 100
            };
            _commandConsumer = new CommandConsumer().InitializeKafka(consumerSetting).Subscribe("CommandTopic");
            _eventConsumer = new DomainEventConsumer().InitializeKafka(consumerSetting).Subscribe("EventTopic");
            _applicationMessageConsumer = new ApplicationMessageConsumer().InitializeKafka(consumerSetting).Subscribe("ApplicationMessageTopic");
            _publishableExceptionConsumer = new PublishableExceptionConsumer().InitializeKafka(consumerSetting).Subscribe("PublishableExceptionTopic");

            _eventConsumer.Start();
            _commandConsumer.Start();
            _applicationMessageConsumer.Start();
            _publishableExceptionConsumer.Start();
            _applicationMessagePublisher.Start();
            _publishableExceptionPublisher.Start();
            _eventPublisher.Start();
            _commandService.Start();

            _isKafkaStarted = true;

            return enodeConfiguration;
        }

        public static ENodeConfiguration UseEventStore(this ENodeConfiguration enodeConfiguration, bool useMockEventStore = false)
        {
            var configuration = enodeConfiguration.GetCommonConfiguration();
            if (useMockEventStore)
            {
                configuration.SetDefault<IEventStore, MockEventStore>();
            }
            else
            {
                enodeConfiguration.UseMongoDbEventStore();
            }
            return enodeConfiguration;
        }

        public static ENodeConfiguration UseKafka(this ENodeConfiguration enodeConfiguration,
                            bool useMockDomainEventPublisher = false,
            bool useMockApplicationMessagePublisher = false,
            bool useMockPublishableExceptionPublisher = false)
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };
            enodeConfiguration.RegisterTopicProviders(assemblies);

            var configuration = enodeConfiguration.GetCommonConfiguration();

            if (useMockDomainEventPublisher)
            {
                configuration.SetDefault<IMessagePublisher<DomainEventStreamMessage>, MockDomainEventPublisher>();
            }
            else
            {
                configuration.SetDefault<IMessagePublisher<DomainEventStreamMessage>, DomainEventPublisher>(_eventPublisher);
            }

            if (useMockApplicationMessagePublisher)
            {
                configuration.SetDefault<IMessagePublisher<IApplicationMessage>, MockApplicationMessagePublisher>();
            }
            else
            {
                configuration.SetDefault<IMessagePublisher<IApplicationMessage>, ApplicationMessagePublisher>(_applicationMessagePublisher);
            }

            if (useMockPublishableExceptionPublisher)
            {
                configuration.SetDefault<IMessagePublisher<IPublishableException>, MockPublishableExceptionPublisher>();
            }
            else
            {
                configuration.SetDefault<IMessagePublisher<IPublishableException>, PublishableExceptionPublisher>(_publishableExceptionPublisher);
            }

            configuration.SetDefault<ICommandService, CommandService>(_commandService);

            return enodeConfiguration;
        }

        public static ENodeConfiguration UsePublishedVersionStore(this ENodeConfiguration enodeConfiguration, bool useMockPublishedVersionStore = false)
        {
            var configuration = enodeConfiguration.GetCommonConfiguration();
            if (useMockPublishedVersionStore)
            {
                configuration.SetDefault<IPublishedVersionStore, MockPublishedVersionStore>();
            }
            else
            {
                enodeConfiguration.UseMongoDbPublishedVersionStore();
            }
            return enodeConfiguration;
        }
    }
}