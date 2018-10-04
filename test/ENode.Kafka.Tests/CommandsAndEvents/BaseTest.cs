using ECommon.Components;
using ECommon.Configurations;
using ECommon.Logging;
using ENode.Commanding;
using ENode.Configurations;
using ENode.Domain;
using ENode.Eventing;
using ENode.Infrastructure;
using System;
using System.Diagnostics;
using System.Reflection;
using ENode.EventStore.MongoDb;
using ECommonConfiguration = ECommon.Configurations.Configuration;
using System.IO;
using Microsoft.Extensions.Configuration;
using System.Linq;
using System.Net;
using System.Collections.Generic;
using ENode.Kafka.Utils;
using System.Net.Sockets;

namespace ENode.Kafka.Tests.CommandsAndEvents
{
    public abstract class BaseTest : IDisposable
    {
        protected IMessagePublisher<IApplicationMessage> _applicationMessagePublisher;
        protected ICommandService _commandService;
        protected IMessagePublisher<DomainEventStreamMessage> _domainEventPublisher;
        protected IEventStore _eventStore;
        protected ILogger _logger;
        protected IMemoryCache _memoryCache;
        protected IMessagePublisher<IPublishableException> _publishableExceptionPublisher;
        protected IPublishedVersionStore _publishedVersionStore;
        private ENodeConfiguration _enodeConfiguration;

        public BaseTest()
        {
            StackTrace st = new StackTrace();
            StackFrame sf = st.GetFrame(0);
            MethodBase currentMethodName = sf.GetMethod();

            Initialize();

            _logger.InfoFormat("----Start to run test: {0}", currentMethodName);

            if (_enodeConfiguration != null)
            {
                CleanupEnode();
            }
        }

        public IConfigurationRoot Root { get; set; }

        public void Dispose()
        {
            if (_enodeConfiguration != null)
            {
                StackTrace st = new StackTrace();
                StackFrame sf = st.GetFrame(0);
                MethodBase currentMethodName = sf.GetMethod();

                _logger.InfoFormat("----Finished test: {0}", currentMethodName);

                CleanupEnode();
            }
        }

        protected void Initialize(
            bool useMockEventStore = false,
            bool useMockPublishedVersionStore = false,
            bool useMockDomainEventPublisher = false,
            bool useMockApplicationMessagePublisher = false,
            bool useMockPublishableExceptionPublisher = false)
        {
            BuildConfiguration();

            InitializeKafka(useMockEventStore,
                useMockPublishedVersionStore,
                useMockDomainEventPublisher,
                useMockApplicationMessagePublisher,
                useMockPublishableExceptionPublisher);
        }

        private void BuildConfiguration()
        {
            var basePath = Directory.GetCurrentDirectory();

            //default setting
            var builder = new ConfigurationBuilder()
                 .SetBasePath(basePath)
                 .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            var environmentName = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
            if (!string.IsNullOrWhiteSpace(environmentName))
            {
                builder = builder.AddJsonFile($"appsettings.{environmentName}.json", optional: true, reloadOnChange: true);
            }

            //external setting for docker
            var configDirName = "docker-config";
            var dirPath = $"{basePath}{Path.DirectorySeparatorChar}{configDirName}";
            if (Directory.Exists(dirPath))
            {
                var skipDirectory = dirPath.Length;
                if (!dirPath.EndsWith("" + Path.DirectorySeparatorChar)) skipDirectory++;
                var fileNames = Directory.EnumerateFiles(dirPath, "*.json", SearchOption.AllDirectories)
                    .Select(f => f.Substring(skipDirectory));
                foreach (var fileName in fileNames)
                {
                    builder = builder.AddJsonFile($"{configDirName}{Path.DirectorySeparatorChar}{fileName}", optional: true, reloadOnChange: true);
                }
            }

            //enviroment variables
            builder.AddEnvironmentVariables();
            Root = builder.Build();
        }

        private void CleanupEnode()
        {
            _enodeConfiguration.Stop();
            _logger.Info("----ENode shutdown.");
        }

        private List<IPEndPoint> GetIPEndPointFromAddresses(string addresses)
        {
            var ipEndPoints = new List<IPEndPoint>();
            var addressList = addresses.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var address in addressList)
            {
                var array = address.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                var hostNameType = Uri.CheckHostName(array[0]);
                IPEndPoint ipEndPoint;
                switch (hostNameType)
                {
                    case UriHostNameType.Dns:
                        ipEndPoint = SocketUtils.GetIPEndPointFromHostName(array[0], int.Parse(array[1]), AddressFamily.InterNetwork, false);
                        break;

                    case UriHostNameType.IPv4:
                    case UriHostNameType.IPv6:
                        ipEndPoint = new IPEndPoint(IPAddress.Parse(array[0]), int.Parse(array[1]));
                        break;

                    case UriHostNameType.Unknown:
                    default:
                        throw new Exception($"Host name type[{hostNameType}] can not resolve.");
                }

                ipEndPoints.Add(ipEndPoint);
            }

            return ipEndPoints;
        }

        private void InitializeKafka(
            bool useMockEventStore = false,
            bool useMockPublishedVersionStore = false,
            bool useMockDomainEventPublisher = false,
            bool useMockApplicationMessagePublisher = false,
            bool useMockPublishableExceptionPublisher = false)
        {
            var brokerAddresses = Root["Kafka:BrokerAddresses"];

            var assemblies = new[]
            {
                Assembly.GetExecutingAssembly()
            };

            _enodeConfiguration = ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .CreateENode()
                .RegisterENodeComponents()
                .UseEventStore(useMockEventStore)
                .UsePublishedVersionStore(useMockPublishedVersionStore)
                .RegisterBusinessComponents(assemblies)
                .InitializeKafka(GetIPEndPointFromAddresses(brokerAddresses))
                .UseKafka(useMockDomainEventPublisher, useMockApplicationMessagePublisher, useMockPublishableExceptionPublisher)
                .BuildContainer();

            var eventStoreConnectionString = Root["ENode:EventStoreConnectionString"];
            var eventStoreDatabase = Root["ENode:EventStoreDatabaseName"];
            if (!useMockEventStore)
            {
                _enodeConfiguration.InitializeMongoDbEventStore(new MongoDbConfiguration()
                {
                    ConnectionString = eventStoreConnectionString,
                    DatabaseName = eventStoreDatabase
                });
            }
            if (!useMockPublishedVersionStore)
            {
                _enodeConfiguration.InitializeMongoDbPublishedVersionStore(new MongoDbConfiguration()
                {
                    ConnectionString = eventStoreConnectionString,
                    DatabaseName = eventStoreDatabase
                });
            }

            var strBrokerAddresses = Root["Kafka:BrokerAddresses"];
            var ipEndPoints = new List<IPEndPoint>();
            var addressList = strBrokerAddresses.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var address in addressList)
            {
                var array = address.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                var endpoint = SocketUtils.GetIPEndPointFromHostName(array[0], int.Parse(array[1]));
                ipEndPoints.Add(endpoint);
            }
            _enodeConfiguration
                .InitializeBusinessAssemblies(assemblies)
                .StartKafka()
                .Start();

            _commandService = ObjectContainer.Resolve<ICommandService>();
            _memoryCache = ObjectContainer.Resolve<IMemoryCache>();
            _eventStore = ObjectContainer.Resolve<IEventStore>();
            _publishedVersionStore = ObjectContainer.Resolve<IPublishedVersionStore>();
            _domainEventPublisher = ObjectContainer.Resolve<IMessagePublisher<DomainEventStreamMessage>>();
            _applicationMessagePublisher = ObjectContainer.Resolve<IMessagePublisher<IApplicationMessage>>();
            _publishableExceptionPublisher = ObjectContainer.Resolve<IMessagePublisher<IPublishableException>>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(typeof(BaseTest));
            _logger.Info("----ENode initialized.");
        }
    }
}