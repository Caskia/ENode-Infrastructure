using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Utilities;
using ENode.Configurations;
using ENode.Eventing;
using ENode.EventStore.MongoDb;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.EventStore.MongoDb.Performance.Tests
{
    internal class Program
    {
        private static ENodeConfiguration _configuration;

        private static MongoDbConfiguration _mongoDbConfiguration = new MongoDbConfiguration()
        {
            ConnectionString = "mongodb://127.0.0.1:20000/eventsotre_test",
            DatabaseName = "eventsotre_test",
        };

        private static void AppendAsyncTest()
        {
            var aggreagateRootId = ObjectId.GenerateNewStringId();
            var count = 50000;
            var printSize = count / 10;
            var eventStore = ObjectContainer.Resolve<IEventStore>();
            var createEventStream = new Func<int, DomainEventStream>(version =>
            {
                var evnt = new TestEvent
                {
                    AggregateRootId = aggreagateRootId,
                    Version = version
                };
                var eventStream = new DomainEventStream(ObjectId.GenerateNewStringId(), aggreagateRootId, "SampleAggregateRootTypeName", version, DateTime.Now, new IDomainEvent[] { evnt });
                return eventStream;
            });

            Console.WriteLine("start to append test, totalCount:" + count);

            var current = 0;
            var watch = Stopwatch.StartNew();
            var waitHandle = new ManualResetEvent(false);

            for (var i = 1; i <= count; i++)
            {
                eventStore.AppendAsync(createEventStream(i)).ContinueWith(t =>
                {
                    if (t.Result.Data == EventAppendResult.DuplicateEvent)
                    {
                        Console.WriteLine("duplicated event stream.");
                        return;
                    }
                    else if (t.Result.Data == EventAppendResult.DuplicateCommand)
                    {
                        Console.WriteLine("duplicated command execution.");
                        return;
                    }
                    var local = Interlocked.Increment(ref current);
                    if (local % printSize == 0)
                    {
                        Console.WriteLine("appended {0}, time:{1}", local, watch.ElapsedMilliseconds);
                        if (local == count)
                        {
                            Console.WriteLine("append throughput: {0} events/s", 1000 * local / watch.ElapsedMilliseconds);
                            waitHandle.Set();
                        }
                    }
                });
            }

            waitHandle.WaitOne();
        }

        private static async Task BatchAppendAsync(BatchAppendContext context)
        {
            var result = await context.EventStore.BatchAppendAsync(context.EventList);

            if (result.Data == EventAppendResult.DuplicateEvent)
            {
                Console.WriteLine("duplicated event stream.");
                return;
            }
            else if (result.Data == EventAppendResult.DuplicateCommand)
            {
                Console.WriteLine("duplicated command execution.");
                return;
            }
            var local = Interlocked.Add(ref context.FinishedCount, context.EventList.Count);
            if (local % context.PrintSize == 0)
            {
                Console.WriteLine("batch appended {0}, time:{1}", local, context.Watch.ElapsedMilliseconds);
            }

            await DoBatchAppendAsync(context);
        }

        private static void BatchAppendAsyncTest()
        {
            Console.WriteLine("");

            var aggreagateRootId = ObjectId.GenerateNewStringId();
            var batchAppendAsyncount = 100000;
            var batchSize = 1000;
            var printSize = batchAppendAsyncount / 10;
            var finishedCount = 0;
            var eventStore = ObjectContainer.Resolve<IEventStore>();
            var eventQueue = new Queue<DomainEventStream>();

            for (var i = 1; i <= batchAppendAsyncount; i++)
            {
                var evnt = new TestEvent
                {
                    AggregateRootId = aggreagateRootId,
                    Version = i
                };
                var eventStream = new DomainEventStream(ObjectId.GenerateNewStringId(), aggreagateRootId, "SampleAggregateRootTypeName", i, DateTime.Now, new IDomainEvent[] { evnt });
                eventQueue.Enqueue(eventStream);
            }

            var watch = Stopwatch.StartNew();
            var context = new BatchAppendContext
            {
                BatchSize = batchSize,
                PrintSize = printSize,
                FinishedCount = finishedCount,
                EventStore = eventStore,
                EventQueue = eventQueue,
                Watch = watch
            };

            Console.WriteLine("start to batch append test, totalCount:" + batchAppendAsyncount);

            DoBatchAppendAsync(context).Wait();
        }

        private static async Task DoBatchAppendAsync(BatchAppendContext context)
        {
            var eventList = new List<DomainEventStream>();

            while (context.EventQueue.Count > 0)
            {
                var evnt = context.EventQueue.Dequeue();
                eventList.Add(evnt);
                if (eventList.Count == context.BatchSize)
                {
                    context.EventList = eventList;
                    await BatchAppendAsync(context);
                    return;
                }
            }

            if (eventList.Count > 0)
            {
                context.EventList = eventList;
                await BatchAppendAsync(context);
            }

            Console.WriteLine("batch append throughput: {0} events/s", 1000 * context.FinishedCount / context.Watch.ElapsedMilliseconds);
        }

        private static void InitializeENodeFramework()
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };

            _configuration = ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler()
                .CreateENode()
                .RegisterENodeComponents()
                .UseMongoDbEventStore();

            _configuration.GetCommonConfiguration()
              .BuildContainer();

            _configuration.InitializeBusinessAssemblies(assemblies)
                .InitializeMongoDbEventStore(_mongoDbConfiguration);

            Console.WriteLine("ENode started...");
        }

        private static void Main(string[] args)
        {
            InitializeENodeFramework();
            AppendAsyncTest();
            BatchAppendAsyncTest();
            Console.ReadLine();
        }

        private class BatchAppendContext
        {
            public int BatchSize;
            public IList<DomainEventStream> EventList;
            public Queue<DomainEventStream> EventQueue;
            public IEventStore EventStore;
            public int FinishedCount;
            public int PrintSize;
            public Stopwatch Watch;
        }

        private class TestEvent : DomainEvent<string>
        { }
    }
}