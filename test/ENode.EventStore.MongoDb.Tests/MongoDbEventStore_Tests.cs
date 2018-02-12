using ECommon.Configurations;
using ENode.Eventing;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using ECommonConfiguration = ECommon.Configurations.Configuration;
using ENode.Configurations;
using System.Reflection;
using ECommon.IO;
using System.Linq;
using ECommon.Utilities;
using ECommon.Components;

namespace ENode.EventStore.MongoDb.Tests
{
    public class MongoDbEventStore_Tests
    {
        private readonly MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://192.168.31.147:27017/eventsotre_test",
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
            var eventStream = GetTestDomainEventStream();

            //Act
            await _store.AppendAsync(eventStream);

            //Assert
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.Version);
            result.Status.ShouldBe(AsyncTaskStatus.Success);
            result.Data.AggregateRootId.ShouldBe(eventStream.AggregateRootId);
            result.Data.AggregateRootTypeName.ShouldBe(eventStream.AggregateRootTypeName);
            result.Data.Version.ShouldBe(eventStream.Version);
            foreach (var @event in result.Data.Events)
            {
                eventStream.Events.ToList().Select(e => e.AggregateRootStringId).ShouldContain(@event.AggregateRootStringId);
            }
            result.Data.CommandId.ShouldBe(eventStream.CommandId);
            result.ShouldNotBeNull();
        }

        [Fact(DisplayName = "Should_Append_Same_EventStream")]
        public async Task Should_Append_Same_EventStream()
        {
            //Arrange
            var eventStream = GetTestDomainEventStream();
            await _store.AppendAsync(eventStream);

            //Act
            await _store.AppendAsync(eventStream);

            //Assert
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.Version);
            result.ShouldNotBeNull();
            result.Status.ShouldBe(AsyncTaskStatus.Success);
        }

        [Fact(DisplayName = "Should_Find_By_Command_Id")]
        public async Task Should_Find_By_Command_Id()
        {
            //Arrange
            var eventStream = GetTestDomainEventStream();
            await _store.AppendAsync(eventStream);

            //Act
            var result = await _store.FindAsync(eventStream.AggregateRootId, eventStream.CommandId);

            //Assert
            result.ShouldNotBeNull();
            result.Status.ShouldBe(AsyncTaskStatus.Success);
            result.Data.AggregateRootId.ShouldBe(eventStream.AggregateRootId);
            result.Data.AggregateRootTypeName.ShouldBe(eventStream.AggregateRootTypeName);
            result.Data.Version.ShouldBe(eventStream.Version);
            foreach (var @event in result.Data.Events)
            {
                eventStream.Events.ToList().Select(e => e.AggregateRootStringId).ShouldContain(@event.AggregateRootStringId);
            }
            result.Data.CommandId.ShouldBe(eventStream.CommandId);
            result.ShouldNotBeNull();
        }

        [Fact(DisplayName = "Should_Query_Aggregate_Events")]
        public async Task Should_Query_Aggregate_Events()
        {
            //Arrange
            var eventStream1 = GetTestDomainEventStream();
            var eventStream2 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", 2, DateTime.Now, new List<DomainEvent<long>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1",
                    Version = 2,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2",
                    Version = 2,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3",
                    Version = 2,
                },
            });
            var eventStream3 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", 3, DateTime.Now, new List<DomainEvent<long>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1",
                    Version = 3,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2",
                    Version = 3,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3",
                    Version = 3,
                },
            });

            await _store.AppendAsync(eventStream1);
            await _store.AppendAsync(eventStream2);
            await _store.AppendAsync(eventStream3);

            //Act
            var events = _store.QueryAggregateEvents(eventStream1.AggregateRootId, eventStream1.AggregateRootTypeName, 1, 3);
            events.Count().ShouldBe(3);
        }

        [Fact(DisplayName = "Should_Query_Aggregate_Events_Async")]
        public async Task Should_Query_Aggregate_Events_Async()
        {
            //Arrange
            var eventStream1 = GetTestDomainEventStream();
            var eventStream2 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", 2, DateTime.Now, new List<DomainEvent<long>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1",
                    Version = 2,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2",
                    Version = 2,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3",
                    Version = 2,
                },
            });
            var eventStream3 = new DomainEventStream(ObjectId.GenerateNewStringId(), eventStream1.AggregateRootId, "typename", 3, DateTime.Now, new List<DomainEvent<long>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1",
                    Version = 3,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2",
                    Version = 3,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3",
                    Version = 3,
                },
            });

            await _store.AppendAsync(eventStream1);
            await _store.AppendAsync(eventStream2);
            await _store.AppendAsync(eventStream3);

            //Act
            var events = await _store.QueryAggregateEventsAsync(eventStream1.AggregateRootId, eventStream1.AggregateRootTypeName, 1, 3);
            events.Data.Count().ShouldBe(3);
        }

        private DomainEventStream GetTestDomainEventStream()
        {
            return new DomainEventStream(ObjectId.GenerateNewStringId(), ObjectId.GenerateNewStringId(), "typename", 1, DateTime.Now, new List<DomainEvent<long>>()
            {
                new Test1DomainEvent()
                {
                    AggregateRootId = 1,
                    Name = "test1",
                    Version = 1,
                },
                new Test2DomainEvent()
                {
                    AggregateRootId = 2,
                    Name = "test2",
                    Version = 1,
                },
                new Test3DomainEvent()
                {
                    AggregateRootId = 3,
                    Name = "test3",
                    Version = 1,
                },
            });
        }
    }

    public class Test1DomainEvent : DomainEvent<long>
    {
        public string Name { get; set; }
    }

    public class Test2DomainEvent : DomainEvent<long>
    {
        public string Name { get; set; }
    }

    public class Test3DomainEvent : DomainEvent<long>
    {
        public string Name { get; set; }
    }
}