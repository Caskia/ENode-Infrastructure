using Autofac;
using Autofac.Extensions.DependencyInjection;
using ECommon.Components;
using ECommon.Configurations;
using ENode.AggregateSnapshot;
using ENode.Configurations;
using ENode.EventStore.MongoDb;
using ENode.Kafka.Utils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Threading;
using ECommonConfiguration = ECommon.Configurations.Configuration;
using ENodeConfiguration = ENode.Configurations.ENodeConfiguration;

namespace ENode.Kafka.Tests
{
    public class ENodeKafkaFixture : IDisposable
    {
        private Assembly[] _bussinessAssemblies;
        private ENodeConfiguration _enodeConfiguration;

        public ENodeKafkaFixture()
        {
            if (ENodeConfiguration.Instance != null)
            {
                CleanupEnode();
            }

            InitializeENode();
        }

        public static IConfigurationRoot Root { get; set; }

        public void Dispose()
        {
            CleanupEnode();
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
            Thread.Sleep(1000);
            ENodeConfiguration.Instance.ShutdownKafka();
            ENodeConfiguration.Instance.Stop();
        }

        private void ConfigureContainer(ContainerBuilder builder)
        {
            BuildConfiguration();

            var brokerAddresses = Root["Kafka:BrokerAddresses"];

            _bussinessAssemblies = new[]
            {
                Assembly.GetExecutingAssembly()
            };

            _enodeConfiguration = ECommonConfiguration
               .Create()
               .UseAutofac(builder)
               .RegisterCommonComponents()
               .UseLog4Net()
               .UseJsonNet()
               .RegisterUnhandledExceptionHandler()
               .CreateENode(new ConfigurationSetting()
               {
                   ProcessTryToRefreshAggregateIntervalMilliseconds = 1000
               })
               .RegisterENodeComponents()
               .UseEventStore(false)
               .UsePublishedVersionStore(false)
               .UseAggregateSnapshot(false)
               .RegisterBusinessComponents(_bussinessAssemblies)
               .InitializeKafka(GetIPEndPointFromAddresses(brokerAddresses))
               .UseKafka(false, false, false);
        }

        private void ConfigureService(IServiceCollection services)
        {
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

        private void InitializeENode()
        {
            var services = new ServiceCollection();
            ConfigureService(services);

            var serviceProviderFactory = new AutofacServiceProviderFactory();
            var containerBuilder = serviceProviderFactory.CreateBuilder(services);

            ConfigureContainer(containerBuilder);

            var serviceProvider = serviceProviderFactory.CreateServiceProvider(containerBuilder);
            ObjectContainer.Current.SetContainer(serviceProvider.GetAutofacRoot());

            ENodeConfiguration.Instance.InitializeBusinessAssemblies(_bussinessAssemblies);

            var eventStoreConnectionString = Root["ENode:EventStoreConnectionString"];
            var eventStoreDatabase = Root["ENode:EventStoreDatabaseName"];
            ENodeConfiguration.Instance
                .InitializeMongoDbEventStore(new MongoDbConfiguration()
                {
                    ConnectionString = eventStoreConnectionString,
                    DatabaseName = eventStoreDatabase
                })
                .InitializeMongoDbPublishedVersionStore(new MongoDbConfiguration()
                {
                    ConnectionString = eventStoreConnectionString,
                    DatabaseName = eventStoreDatabase
                })
                .InitializeMongoDbAggregateSnapshotter(new MongoDbConfiguration()
                {
                    ConnectionString = eventStoreConnectionString,
                    DatabaseName = eventStoreDatabase
                })
                .StartKafka()
                .Start();
        }
    }
}