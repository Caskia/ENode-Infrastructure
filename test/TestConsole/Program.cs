using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
using ENode.Lock.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            ConnectionString = "redis:20002,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
            DatabaseId = 3
        };

        private static async Task Main(string[] args)
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

            var listDics = new List<(int index, Dictionary<int, int> dic)>();
            for (int i = 0; i < 10; i++)
            {
                listDics.Add((i, new Dictionary<int, int>()));
            }
            var tasks = new List<Task>();
            var stopWatch = new Stopwatch();

            var lockKey = Guid.NewGuid().ToString();
            stopWatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                foreach (var item in listDics)
                {
                    tasks.Add(_lockService.ExecuteInLockAsync($"{lockKey}-{item.index}", async () =>
                     {
                         await Task.Delay(10);
                         item.dic.Add(item.dic.Count, Thread.CurrentThread.GetHashCode());
                         Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss,fff")} {Thread.CurrentThread.GetHashCode()} {item.dic.Count} complete");
                     }));
                }
            }
            await Task.WhenAll(tasks);
            stopWatch.Stop();

            Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss,fff")} all complete use time[{stopWatch.ElapsedMilliseconds}]");
            await Task.Delay(60000);
            Console.ReadKey();
        }
    }
}