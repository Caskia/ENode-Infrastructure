using ECommon.Components;
using ENode.Configurations;
using System;

namespace ENode.Lock.Redis
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Initialize the RedisLockService with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="connectionString"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeRedisLockService(this ENodeConfiguration eNodeConfiguration,
            RedisOptions redisOptions,
            string keyPrefix = "default",
            TimeSpan? timeout = null,
            TimeSpan? holdDuration = null)
        {
            ((RedLockService)ObjectContainer.Resolve<ILockService>()).Initialize(redisOptions, keyPrefix, timeout, holdDuration);
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the RedisLockService as the ILockService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseRedisLockService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ILockService, RedLockService>();
            return eNodeConfiguration;
        }
    }
}