using ECommon.Components;
using ECommon.Configurations;
using ECommon.Utilities;
using ENode.AggregateSnapshot;
using ENode.Configurations;
using ENode.Eventing;
using ENode.EventStore.MongoDb.Tests.Domain;
using ENode.Infrastructure;
using Shouldly;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.EventStore.MongoDb.Tests
{
    public class MongoDbPublishedVersionStore_Tests
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://127.0.0.1:20000/eventsotre_test?maxPoolSize=500",
            DatabaseName = "eventsotre_test",
        };

        private readonly IPublishedVersionStore _store;
        private readonly ITypeNameProvider _typeNameProvider;

        public MongoDbPublishedVersionStore_Tests()
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
                .UseMongoDbPublishedVersionStore()
                .UseMongoDbAggregateSnapshotter();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeMongoDbPublishedVersionStore(_mongoDbConfiguration)
                .InitializeMongoDbAggregateSnapshotter(_mongoDbConfiguration);

            _store = ObjectContainer.Resolve<IPublishedVersionStore>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
        }

        [Fact(DisplayName = "Should_Insert_Published_Version")]
        public async Task Should_Insert_Published_Version()
        {
            //Arrange
            var processName = "test1";
            var aggregateId = ObjectId.GenerateNewStringId();
            var aggregateTypeName = _typeNameProvider.GetTypeName(typeof(Product));
            var version = 1;

            //Act
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);

            //Assert
            var publishedVersion = await _store.GetPublishedVersionAsync(processName, aggregateTypeName, aggregateId);
            publishedVersion.ShouldBe(version);
        }

        [Fact(DisplayName = "Should_Insert_Published_Version")]
        public async Task Should_Insert_Published_Version_Concurrent()
        {
            var tasks = new List<Task>();

            Parallel.For(0, 50000, async i =>
            {
                var processName = "test" + i;
                var aggregateId = ObjectId.GenerateNewStringId();
                var aggregateTypeName = _typeNameProvider.GetTypeName(typeof(Product));
                var version = 1;

                await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);
            });

            //Parallel.For(0, 100, async i =>
            //{
            //    for (int j = 0; j < 500; j++)
            //    {
            //        var processName = "test" + i + "-" + j;
            //        var aggregateId = ObjectId.GenerateNewStringId();
            //        var aggregateTypeName = ObjectId.GenerateNewStringId();
            //        var version = 1;

            //        await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);
            //    }
            //});
        }

        [Fact(DisplayName = "Should_Update_Published_Version")]
        public async Task Should_Update_Published_Version()
        {
            //Arrange
            var processName = "test1";
            var aggregateId = ObjectId.GenerateNewStringId();
            var aggregateTypeName = _typeNameProvider.GetTypeName(typeof(Product));
            var version = 1;

            //Act
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version + 1);

            //Assert
            var publishedVersion = await _store.GetPublishedVersionAsync(processName, aggregateTypeName, aggregateId);
            publishedVersion.ShouldBe(version + 1);
        }
    }
}