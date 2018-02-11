using ECommon.Configurations;
using ENode.Configurations;
using Shouldly;
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

        private RedisLockService _redisLockService;

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
                .RegisterENodeComponents();

            enode.GetCommonConfiguration()
              .BuildContainer();

            enode.InitializeBusinessAssemblies(assemblies);

            _redisLockService = new RedisLockService();
            _redisLockService.Initialize(_redisOptions);
        }

        [Fact(DisplayName = "Should_Execute_In_Lock_By_Multiple_Threads")]
        public void Should_Execute_In_Lock_By_Multiple_Threads()
        {
            //Arrange
            var hs = new HashSet<int>();

            //Act
            Parallel.For(0, 10, i =>
            {
                _redisLockService.ExecuteInLock("test", () =>
                {
                    hs.Add(i);
                });
            });

            //Assert
            hs.Count.ShouldBe(100);
        }
    }
}