using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using ENode.Infrastructure;
using Shouldly;
using System;
using System.Collections.Generic;
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
            ConnectionString = "192.168.31.147:6379,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
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
                .InitializeRedisLockService(_redisOptions);

            _lockService = ObjectContainer.Resolve<ILockService>();
        }

        [Fact(DisplayName = "Should_Execute_In_Lock_By_Multiple_Threads")]
        public void Should_Execute_In_Lock_By_Multiple_Threads()
        {
            //Arrange
            ThreadPool.SetMinThreads(60, 60);
            ThreadPool.SetMaxThreads(60, 60);
            var hs = new HashSet<int>();

            //Act
            Parallel.For(0, 100, i =>
            {
                _lockService.ExecuteInLock("test", () =>
                {
                    hs.Add(i);
                });
            });

            //Assert
            hs.Count.ShouldBe(50);
        }

        [Fact(DisplayName = "Should_Redis_Set_Multiple_Threads")]
        public void Should_Redis_Set_Multiple_Threads()
        {
            var redisProvider = new RedisProvider(_redisOptions);
            var database = redisProvider.GetDatabase();
            var tasks = new List<Task>();
            for (int i = 0; i < 200000; i++)
            {
                tasks.Add(Task.Factory.StartNew(t =>
                {
                    //_lockService.ExecuteInLock("test", () =>
                    //{
                    //});
                    database.StringSetAsync(t.ToString(), "test", TimeSpan.FromSeconds(30)).Wait();
                }, i));
            }

            Task.WaitAll(tasks.ToArray());

            Thread.Sleep(2000);
        }
    }
}