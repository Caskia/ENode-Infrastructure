using ECommon.Configurations;
using ENode.AggregateSnapshot.Configuration;
using ENode.Configurations;
using System;
using System.Reflection;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.AggregateSnapshot.Tests
{
    public abstract class TestBase : IDisposable
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://127.0.0.1:20000/aggregatesnapshot_test",
            DatabaseName = "aggregatesnapshot_test",
        };

        public TestBase()
        {
            Initialize();
        }

        public void Dispose()
        {
            Clean();
        }

        private void Clean()
        {
        }

        private void Initialize()
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };

            var enode = ECommonConfiguration.Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .CreateENode(new ConfigurationSetting())
                .RegisterBusinessComponents(assemblies)
                .RegisterENodeComponents()
                .UseMongoDbAggregateSnapshotter();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeMongoDbAggregateSnapshotter(_mongoDbConfiguration);
        }
    }
}