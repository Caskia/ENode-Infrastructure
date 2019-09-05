using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using Shouldly;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.Lock.Redis.Tests
{
    public class RedisLockService_Tests
    {
        private readonly RedisOptions _redisOptions = new RedisOptions()
        {
            ConnectionString = "127.0.0.1:20002,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
            DatabaseId = 3
        };

        private ILockService _lockService;

        public RedisLockService_Tests()
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
                .UseRedisLockService();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies)
                .InitializeRedisLockService(_redisOptions, timeout: TimeSpan.FromSeconds(5));

            _lockService = ObjectContainer.Resolve<ILockService>();
        }

        [Fact]
        public async Task Should_AquireLock_Timeout()
        {
            var lockKey = Guid.NewGuid().ToString();

            var tasks = new List<Task>();

            for (int i = 0; i < 2; i++)
            {
                tasks.Add(_lockService.ExecuteInLockAsync(lockKey, async () =>
                {
                    await Task.Delay(5 * 1000);
                }));
            }

            await Assert.ThrowsAsync<DistributedLockTimeoutException>(async () => await Task.WhenAll(tasks));
        }

        [Fact]
        public async Task Should_Execute_Async()
        {
            //Act
            await _lockService.ExecuteInLockAsync("test", async () =>
            {
                await Task.Delay(3000);
            });
        }

        [Fact]
        public async Task Should_Execute_In_Different_Lock_By_Multiple_Threads()
        {
            ThreadPool.SetMinThreads(1, 1);
            ThreadPool.SetMaxThreads(4, 4);

            //Arrange
            Func<string, Task> func = async lockKey =>
            {
                await _lockService.ExecuteInLockAsync(lockKey, async () =>
                {
                    await Task.Delay(1000);
                });
            };
            var tasks = new List<Task>();

            //Act
            for (int i = 0; i < 10; i++)
            {
                var lockKey = $"{i}-{Guid.NewGuid().ToString()}-{Guid.NewGuid().ToString()}";
                tasks.Add(func(lockKey));
            }

            await Task.WhenAll(tasks);
        }

        [Fact]
        public async Task Should_Execute_In_Lock_By_Multiple_Threads()
        {
            //Arrange
            var redisProvider = new RedisProvider(_redisOptions);
            var tasks = new List<Task<(int RetryCount, DateTime BeginTime, DateTime EndTime, TimeSpan WaitTimeSpan)>>();
            var results = new List<(int RetryCount, DateTime BeginTime, DateTime EndTime, TimeSpan WaitTimeSpan)>();
            var dic = new Dictionary<int, int>();
            var stopWatch = new Stopwatch();
            var lockKey = Guid.NewGuid().ToString();

            var WaitAndRetryFunc = default(Func<int, DateTime?, Task<(int RetryCount, DateTime BeginTime, DateTime EndTime, TimeSpan WaitTimeSpan)>>);
            WaitAndRetryFunc = async (retryCount, beginTime) =>
            {
                if (!beginTime.HasValue)
                {
                    beginTime = DateTime.Now;
                }

                try
                {
                    await _lockService.ExecuteInLockAsync(lockKey, async () =>
                    {
                        //await Task.Yield();
                        await Task.Delay(10);
                        dic.Add(dic.Count + 1, Thread.CurrentThread.GetHashCode());
                    });

                    var now = DateTime.Now;
                    var waitTimeSpan = now - beginTime.Value;
                    return (retryCount, beginTime.Value, DateTime.Now, waitTimeSpan);
                }
                catch (DistributedLockTimeoutException)
                {
                    retryCount++;
                    return await WaitAndRetryFunc(retryCount, beginTime);
                }
            };

            //Act
            stopWatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                //results.Add(await WaitAndRetryFunc(0, null));
                tasks.Add(WaitAndRetryFunc(0, null));
            }
            await Task.WhenAll(tasks);
            stopWatch.Stop();

            //Assert
            results = tasks.Select(t => t.Result).ToList();
            var retryMostTasks = results.Where(t => t.RetryCount == results.Max(l => l.RetryCount)).ToList();
            var waitLongestTaks = results.Where(t => t.WaitTimeSpan == results.Max(l => l.WaitTimeSpan)).ToList();
            //System.IO.File.WriteAllText(@"C:\Users\Caskia\Desktop\1.txt", $"retryCount:{retryMostTasks.FirstOrDefault().RetryCount}, waittime:{waitLongestTaks.FirstOrDefault().WaitTimeSpan.TotalMilliseconds}");
            dic.Count.ShouldBe(1000);
        }

        [Fact]
        public async Task Should_Redis_Set_By_Multiple_Threads()
        {
            var redisProvider = new RedisProvider(_redisOptions);
            var database = redisProvider.GetDatabase();
            var tasks = new List<Task>();
            var dic = new Dictionary<int, int>();

            for (int i = 0; i < 10000; i++)
            {
                //tasks.Add(Task.Factory.StartNew(t => RunLock(database, (int)t), i));

                tasks.Add(RunLockAsync(database, i));

                //tasks.Add(Task.Factory.StartNew(t => database.StringSet(t.ToString(), "test", TimeSpan.FromSeconds(30)), i));

                //tasks.Add(database.StringSetAsync(i.ToString(), "test", TimeSpan.FromSeconds(30)));
            }

            await Task.WhenAll(tasks);

            await Task.Delay(5000);
        }

        private void RunLock(IDatabase database, int t)
        {
            var value = t.ToString();
            database.LockTake(value, value, TimeSpan.FromSeconds(30));
            database.LockRelease(value, value);
        }

        private async Task RunLockAsync(IDatabase database, int t)
        {
            var value = t.ToString();
            await database.LockTakeAsync(value, value, TimeSpan.FromSeconds(30));
            await database.LockReleaseAsync(value, value);
        }
    }
}