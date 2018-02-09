using ECommon.Configurations;
using ENode.Eventing;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using ECommonConfiguration = ECommon.Configurations.Configuration;
using ENode.Configurations;

namespace ENode.EventStore.MongoDb.Tests
{
    public class MongoDbEventStore_Tests
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://192.168.31.147:27017/test",
            DatabaseName = "test",
        };

        public MongoDbEventStore_Tests()
        {
            ECommonConfiguration.Create()
              .UseAutofac()
              .RegisterCommonComponents()
              .UseLog4Net()
              .UseJsonNet()
              .CreateENode(new ConfigurationSetting())
              .RegisterENodeComponents()
              .GetCommonConfiguration()
              .BuildContainer();
        }

        [Fact(DisplayName = "Should_Append_EventStream")]
        public async Task Should_Append_EventStream()
        {
            //Arrange
            var store = new MongoDbEventStore();
            store.Initialize(_mongoDbConfiguration);
            var eventStream = GetTestDomainEventStream();

            //Act
            await store.AppendAsync(eventStream);

            //Assert
            var result = store.FindAsync(eventStream.AggregateRootId, eventStream.Version);
            result.ShouldNotBeNull();
        }

        private DomainEventStream GetTestDomainEventStream()
        {
            return new DomainEventStream("1", "1", "typename", 1, DateTime.Now, new List<TestDomainEvent>()
            {
                new TestDomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1"
                },
                new TestDomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2"
                },
                new TestDomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3"
                },
            });
        }
    }

    public class TestDomainEvent : DomainEvent<long>
    {
        public string Name { get; set; }
    }
}