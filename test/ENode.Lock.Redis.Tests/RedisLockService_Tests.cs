using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using ENode.Infrastructure;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Reflection;
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
            var list = new List<int>();

            //Act
            Parallel.For(0, 50, i =>
            {
                _lockService.ExecuteInLock("test", () =>
                {
                    list.Add(i);
                });
            });

            //Assert
            list.Count.ShouldBe(50);
        }
    }
}