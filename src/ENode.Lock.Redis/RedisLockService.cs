using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using ENode.Infrastructure;
using StackExchange.Redis;
using System;

namespace ENode.Lock.Redis
{
    public class RedisLockService : ILockService
    {
        #region Private Variables

        private TimeSpan _holdDurationTimeSpan = TimeSpan.FromSeconds(30);
        private ILogger _logger;
        private RedisOptions _redisOptions;
        private RedisProvider _redisProvider;
        private TimeSpan _timeoutTimeSpan = TimeSpan.FromSeconds(30);

        #endregion Private Variables

        #region Public Methods

        public void AddLockKey(string lockKey)
        {
            throw new NotImplementedException("There is no need to add lock key when use redis lock service.");
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            using (RedisLock.Acquire(_redisProvider.GetDatabase(), GetRedisKey(lockKey), _timeoutTimeSpan, _holdDurationTimeSpan))
            {
                action();
            }
        }

        public RedisLockService Initialize(
            RedisOptions redisOptions,
            TimeSpan? timeout = null,
            TimeSpan? holdDuration = null
            )
        {
            _redisOptions = redisOptions;

            Ensure.NotNull(_redisOptions, "_redisOptions");
            Ensure.NotNull(_redisOptions.ConnectionString, "_redisOptions.ConnectionString");
            Ensure.Positive(_redisOptions.DatabaseId, "_redisOptions.DatabaseId");

            if (timeout.HasValue)
            {
                _timeoutTimeSpan = timeout.Value;
            }

            if (holdDuration.HasValue)
            {
                _holdDurationTimeSpan = holdDuration.Value;
            }

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _redisProvider = new RedisProvider(_redisOptions);

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