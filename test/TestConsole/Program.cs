using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using ENode.Lock.Redis;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace TestConsole
{
    public class Program
    {
        private static ILockService _lockService;

        private static RedisOptions _redisOptions = new RedisOptions()
        {
            ConnectionString = "192.168.31.125:20002,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
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

            var redisProvider = new RedisProvider(_redisOptions);
            var database = redisProvider.GetDatabase();
            var tasks = new List<Task>();
            var dic = new Dictionary<int, int>();

            Parallel.For(0, 200000, async i =>
            {
                await database.StringSetAsync(i.ToString(), "test", TimeSpan.FromSeconds(30));
            });

            //var dic = new Dictionary<int, int>();
            //var tasks = new List<Task>();

            //for (int i = 0; i < 300; i++)
            //{
            //    tasks.Add(
            //        _lockService.ExecuteInLockAsync("test", () =>
            //        {
            //            dic.Add(dic.Count + 1, System.Threading.Thread.CurrentThread.GetHashCode());
            //        })
            //    );
            //}

            //Task.WaitAll(tasks.ToArray());

            Console.ReadKey();
        }
    }
}