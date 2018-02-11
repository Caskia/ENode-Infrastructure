using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using ENode.Infrastructure;
using ENode.Lock.Redis.Exceptions;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;

namespace ENode.Lock.Redis
{
    public class RedisLockService : ILockService
    {
        #region Private Variables

        private static readonly TimeSpan DefaultExpiriesTimeSpan = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan DefaultRetryTimeSpan = TimeSpan.FromMilliseconds(1);
        private static readonly TimeSpan DefaultWaitTimeSpan = TimeSpan.FromMilliseconds(10);
        private ILogger _logger;
        private RedisOptions _redisOptions;
        private RedisProvider _redisProvider;
        private RedLockFactory _redLockFactory;

        #endregion Private Variables

        #region Public Methods

        public void AddLockKey(string lockKey)
        {
            throw new NotImplementedException("There is no need to add lock key when use redis lock service.");
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            using (var redLock = _redLockFactory.CreateLock(GetRedisKey(lockKey), DefaultExpiriesTimeSpan, DefaultWaitTimeSpan, DefaultRetryTimeSpan)) // there are also non async Create() methods
            {
                if (redLock.IsAcquired)
                {
                    action();
                }
                else
                {
                    throw new DistributedLockAcquireException(lockKey);
                }
            }
        }

        public RedisLockService Initialize(RedisOptions redisOptions)
        {
            _redisOptions = redisOptions;

            Ensure.NotNull(_redisOptions, "_redisOptions");
            Ensure.NotNull(_redisOptions.ConnectionString, "_redisOptions.ConnectionString");
            Ensure.Positive(_redisOptions.DatabaseId, "_redisOptions.DatabaseId");

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _redisProvider = new RedisProvider(_redisOptions);
            _redLockFactory = RedLockFactory.Create(new List<RedLockMultiplexer>
            {
                _redisProvider.CreateConnectionMultiplexer()
            });

            return this;
        }

        #endregion Public Methods

        #region Private Methods

        private RedisKey GetRedisKey(string key)
        {
            return $"enode:lock:{key}";
        }

        #endregion Private Methods
    }
}