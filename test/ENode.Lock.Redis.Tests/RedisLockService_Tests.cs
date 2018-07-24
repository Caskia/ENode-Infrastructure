﻿using ECommon.Components;
using ECommon.Configurations;
using ENode.Configurations;
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
        private static object lockObj = new object();

        private readonly RedisOptions _redisOptions = new RedisOptions()
        {
            ConnectionString = "192.168.31.125:20002,keepAlive=60,abortConnect=false,connectTimeout=5000,syncTimeout=5000",
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
        public async Task Should_Execute_In_Lock_By_Multiple_Threads()
        {
            //Arrange
            var redisProvider = new RedisProvider(_redisOptions);
            var tasks = new List<Task>();
            var dic = new Dictionary<int, int>();

            //Act
            for (int i = 0; i < 400; i++)
            {
                tasks.Add(_lockService.ExecuteInLockAsync("test", () =>
                {
                    dic.Add(dic.Count + 1, System.Threading.Thread.CurrentThread.GetHashCode());
                }));
            }
            await Task.WhenAll(tasks);

            //Assert
            dic.Count.ShouldBe(400);
        }

        [Fact(DisplayName = "Should_Redis_Set_By_Multiple_Threads")]
        public async Task Should_Redis_Set_By_Multiple_Threads()
        {
            var redisProvider = new RedisProvider(_redisOptions);
            var database = redisProvider.GetDatabase();
            var tasks = new List<Task>();
            var dic = new Dictionary<int, int>();

            for (int i = 0; i < 20000; i++)
            {
                tasks.Add(database.StringSetAsync(i.ToString(), "test", TimeSpan.FromSeconds(30)));
            }

            await Task.WhenAll(tasks);
        }
    }
}