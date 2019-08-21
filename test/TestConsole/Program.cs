using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using ENode.Lock.Redis;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace TestConsole
{
    public class Program
    {
        private static ILockService _lockService;

        private static RedisOptions _redisOptions = new RedisOptions()
        {
            ConnectionString = "127.0.0.1:20002,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
            DatabaseId = 3
        };

        private static void Main(string[] args)
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

            var dic = new Dictionary<int, int>();
            var tasks = new List<Task>();

            for (int i = 0; i < 10000; i++)
            {
                tasks.Add(
                _lockService.ExecuteInLockAsync("test", async () =>
                 {
                     dic.Add(dic.Count, Thread.CurrentThread.GetHashCode());
                     await Task.Delay(10);
                 }));
            }

            Task.WhenAll(tasks);

            Console.ReadKey();
        }
    }
}