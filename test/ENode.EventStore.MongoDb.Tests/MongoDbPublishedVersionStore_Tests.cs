using ECommon.Configurations;
using ECommon.Utilities;
using ENode.Configurations;
using System.Reflection;
using System.Threading.Tasks;
using ECommonConfiguration = ECommon.Configurations.Configuration;
using Shouldly;
using ECommon.IO;
using Xunit;
using ECommon.Components;
using ENode.Infrastructure;

namespace ENode.EventStore.MongoDb.Tests
{
    public class MongoDbPublishedVersionStore_Tests
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://192.168.31.147:27017/eventsotre_test",
            DatabaseName = "eventsotre_test",
        };

        private readonly IPublishedVersionStore _store;

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
                .UseMongoDbPublishedVersionStore();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeMongoDbPublishedVersionStore(_mongoDbConfiguration);

            _store = ObjectContainer.Resolve<IPublishedVersionStore>();
        }

        [Fact(DisplayName = "Should_Insert_Published_Version")]
        public async Task Should_Insert_Published_Version()
        {
            //Arrange
            var processName = "test1";
            var aggregateId = ObjectId.GenerateNewStringId();
            var aggregateTypeName = ObjectId.GenerateNewStringId();
            var version = 1;

            //Act
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);

            //Assert
            var publishedVersion = await _store.GetPublishedVersionAsync(processName, aggregateTypeName, aggregateId);
            publishedVersion.Status.ShouldBe(AsyncTaskStatus.Success);
            publishedVersion.Data.ShouldBe(version);
        }

        [Fact(DisplayName = "Should_Update_Published_Version")]
        public async Task Should_Update_Published_Version()
        {
            //Arrange
            var processName = "test1";
            var aggregateId = ObjectId.GenerateNewStringId();
            var aggregateTypeName = ObjectId.GenerateNewStringId();
            var version = 1;

            //Act
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version);
            await _store.UpdatePublishedVersionAsync(processName, aggregateTypeName, aggregateId, version + 1);

            //Assert
            var publishedVersion = await _store.GetPublishedVersionAsync(processName, aggregateTypeName, aggregateId);
            publishedVersion.Status.ShouldBe(AsyncTaskStatus.Success);
            publishedVersion.Data.ShouldBe(version + 1);
        }
    }
}