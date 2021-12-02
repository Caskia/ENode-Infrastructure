using ECommon.Configurations;
using ENode.Configurations;
using System;
using System.Reflection;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.AggregateSnapshot.Tests
{
    public abstract class TestBase : IDisposable
    {
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
                .UseLog4Net("log4net.config", "testRepository")
                .UseJsonNet()
                .CreateENode(new ConfigurationSetting())
                .RegisterBusinessComponents(assemblies)
                .RegisterENodeComponents()
                .UseMySqlAggregateSnapshotter();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeMySqlAggregateSnapshotter(
                "Datasource=mysql;Database=eventstore;uid=root;pwd=admin!@#;Allow User Variables=True;AutoEnlist=false;"
                );
        }
    }
}