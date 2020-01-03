using ECommon.Components;
using ECommon.Configurations;
using ECommon.IO;
using ECommon.Utilities;
using ENode.Configurations;
using ENode.Eventing;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.EventStore.MongoDb.Tests
{
    public class MongoDbEventStore_Tests
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://127.0.0.1:20000/eventsotre_test",
            DatabaseName = "eventsotre_test",
        };

        private readonly IEventStore _store;

        public MongoDbEventStore_Tests()
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
                .UseMongoDbEventStore();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeMongoDbEventStore(_mongoDbConfiguration);

            _store = ObjectContainer.Resolve<IEventStore>();
        }

        [Fact(DisplayName = "Should_Append_EventStream")]
        public async Task Should_Append_EventStream()
        {
            //Arrange
            var eventStream = GetTestDomainEventStream(ObjectId.GenerateNewStringId());

            //Act
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream });

            //Assert
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.Version);
            result.AggregateRootId.ShouldBe(eventStream.AggregateRootId);
            result.AggregateRootTypeName.ShouldBe(eventStream.AggregateRootTypeName);
            result.Version.ShouldBe(eventStream.Version);
            foreach (var @event in result.Events)
            {
                eventStream.Events.ToList().Select(e => e.AggregateRootStringId).ShouldContain(@event.AggregateRootStringId);
            }
            result.CommandId.ShouldBe(eventStream.CommandId);
            result.ShouldNotBeNull();
        }

        [Fact(DisplayName = "Should_Append_Same_EventStream")]
        public async Task Should_Append_Same_EventStream()
        {
            //Arrange
            var eventStream = GetTestDomainEventStream(ObjectId.GenerateNewStringId());
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream });

            //Act
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream });

            //Assert
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.Version);
            result.ShouldNotBeNull();
        }

        [Fact(DisplayName = "Should_Find_By_Command_Id")]
        public async Task Should_Find_By_Command_Id()
        {
            //Arrange
            var eventStream = GetTestDomainEventStream(ObjectId.GenerateNewStringId());
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream });

            //Act
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.CommandId);

            //Assert
            result.ShouldNotBeNull();
            result.AggregateRootId.ShouldBe(eventStream.AggregateRootId);
            result.AggregateRootTypeName.ShouldBe(eventStream.AggregateRootTypeName);
            result.Version.ShouldBe(eventStream.Version);
            foreach (var @event in result.Events)
            {
                eventStream.Events.ToList().Select(e => e.AggregateRootStringId).ShouldContain(@event.AggregateRootStringId);
            }
            result.CommandId.ShouldBe(eventStream.CommandId);
            result.ShouldNotBeNull();
        }

        [Fact(DisplayName = "Should_Query_Aggregate_Events_Async")]
        public async Task Should_Query_Aggregate_Events_Async()
        {
            //Arrange
            var eventStream1 = GetTestDomainEventStream(ObjectId.GenerateNewStringId());
            var eventStream2 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", DateTime.Now, new List<DomainEvent<string>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test1",
                    Version = 2,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test2",
                    Version = 2,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test3",
                    Version = 2,
                },
            });
            var eventStream3 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", DateTime.Now, new List<DomainEvent<string>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test1",
                    Version = 3,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test2",
                    Version = 3,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = eventStream1.AggregateRootId,
                    Name = "test3",
                    Version = 3,
                },
            });

            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream1 });
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream2 });
            await _store.BatchAppendAsync(new List<DomainEventStream> { eventStream3 });

            //Act
            var events = await _store.QueryAggregateEventsAsync(eventStream1.AggregateRootId, eventStream1.AggregateRootTypeName, 1, 3);
            events.Count().ShouldBe(3);
        }

        private DomainEventStream GetTestDomainEventStream(string aggregateRootId)
        {
            return new DomainEventStream(ObjectId.GenerateNewStringId(), aggregateRootId, "typename", DateTime.Now, new List<DomainEvent<string>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = aggregateRootId,
                    Name = "test1",
                    Version = 1,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = aggregateRootId,
                    Name = "test2",
                    Version = 1,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId =aggregateRootId,
                    Name = "test3",
                    Version = 1,
                },
            });
        }
    }

    public class Test1DomainEvent : DomainEvent<string>
    {
        public string Name { get; set; }
    }

    public class Test2DomainEvent : DomainEvent<string>
    {
        public string Name { get; set; }
    }

    public class Test3DomainEvent : DomainEvent<string>
    {
        public string Name { get; set; }
    }
}