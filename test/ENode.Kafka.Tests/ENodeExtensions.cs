using ENode.AggregateSnapshot;
using ENode.Commanding;
using ENode.Configurations;
using ENode.Domain;
using ENode.Eventing;
using ENode.EventStore.MongoDb;
using ENode.Kafka.Consumers;
using ENode.Kafka.Producers;
using ENode.Kafka.Tests.CommandsAndEvents.Mocks;
using ENode.Kafka.Utils;
using ENode.Messaging;
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
        private static CommandResultProcessor _commandResultProcessor;
        private static CommandService _commandService;
        private static DomainExceptionConsumer _domainExceptionConsumer;
        private static DomainExceptionPublisher _domainExceptionPublisher;
        private static DomainEventConsumer _eventConsumer;
        private static DomainEventPublisher _eventPublisher;

        public static ENodeConfiguration InitializeKafka(this ENodeConfiguration enodeConfiguration, List<IPEndPoint> brokerEndPoints)
        {
            _brokerEndPoints = brokerEndPoints;

            return enodeConfiguration;
        }

        public static ENodeConfiguration ShutdownKafka(this ENodeConfiguration enodeConfiguration)
        {
            _commandService.Shutdown();
            _eventPublisher.Shutdown();
            _applicationMessagePublisher.Shutdown();
            _domainExceptionPublisher.Shutdown();

            _commandConsumer.Shutdown();
            _eventConsumer.Shutdown();
            _applicationMessageConsumer.Shutdown();
            _domainExceptionConsumer.Shutdown();

            return enodeConfiguration;
        }

        public static ENodeConfiguration StartKafka(this ENodeConfiguration enodeConfiguration)
        {
            var produceSetting = new ProducerSetting()
            {
                BrokerEndPoints = _brokerEndPoints
            };
            var consumerSetting = new ConsumerSetting()
            {
                BrokerEndPoints = _brokerEndPoints,
                CommitConsumerOffsetInterval = 100
            };

            _commandConsumer = new CommandConsumer()
                .InitializeKafka(consumerSetting)
                .Subscribe("CommandTopic");
            _eventConsumer = new DomainEventConsumer()
                .InitializeKafka(consumerSetting)
                .Subscribe("EventTopic");
            _applicationMessageConsumer = new ApplicationMessageConsumer()
                .InitializeKafka(consumerSetting)
                .Subscribe("ApplicationMessageTopic");
            _domainExceptionConsumer = new DomainExceptionConsumer()
                .InitializeKafka(consumerSetting)
                .Subscribe("DomainExceptionTopic");

            _commandResultProcessor = new CommandResultProcessor()
               .Initialize(new IPEndPoint(SocketUtils.GetLocalIPV4(), 9003));
            _commandService.InitializeKafka(produceSetting, _commandResultProcessor);
            _eventPublisher.InitializeKafka(produceSetting);
            _applicationMessagePublisher.InitializeKafka(produceSetting);
            _domainExceptionPublisher.InitializeKafka(produceSetting);

            _commandConsumer.Start();
            _eventConsumer.Start();
            _applicationMessageConsumer.Start();
            _domainExceptionConsumer.Start();

            _commandService.Start();
            _eventPublisher.Start();
            _applicationMessagePublisher.Start();
            _domainExceptionPublisher.Start();

            return enodeConfiguration;
        }

        public static ENodeConfiguration UseAggregateSnapshot(this ENodeConfiguration enodeConfiguration, bool useMockPublishedVersionStore = false)
        {
            var configuration = enodeConfiguration.GetCommonConfiguration();
            if (!useMockPublishedVersionStore)
            {
                enodeConfiguration.UseMongoDbAggregateSnapshotter();
            }
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
            bool useMockDomainExceptionPublisher = false)
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };
            enodeConfiguration.RegisterTopicProviders(assemblies);

            var configuration = enodeConfiguration.GetCommonConfiguration();

            _commandService = new CommandService();
            configuration.SetDefault<ICommandService, CommandService>(_commandService);

            if (useMockDomainEventPublisher)
            {
                configuration.SetDefault<IMessagePublisher<DomainEventStreamMessage>, MockDomainEventPublisher>();
            }
            else
            {
                _eventPublisher = new DomainEventPublisher();
                configuration.SetDefault<IMessagePublisher<DomainEventStreamMessage>, DomainEventPublisher>(_eventPublisher);
            }

            if (useMockApplicationMessagePublisher)
            {
                configuration.SetDefault<IMessagePublisher<IApplicationMessage>, MockApplicationMessagePublisher>();
            }
            else
            {
                _applicationMessagePublisher = new ApplicationMessagePublisher();
                configuration.SetDefault<IMessagePublisher<IApplicationMessage>, ApplicationMessagePublisher>(_applicationMessagePublisher);
            }

            if (useMockDomainExceptionPublisher)
            {
                configuration.SetDefault<IMessagePublisher<IDomainException>, MockDomainExceptionPublisher>();
            }
            else
            {
                _domainExceptionPublisher = new DomainExceptionPublisher();
                configuration.SetDefault<IMessagePublisher<IDomainException>, DomainExceptionPublisher>(_domainExceptionPublisher);
            }

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