using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class RedLockService : ILockService
    {
        #region Private Variables

        private TimeSpan _expiries = TimeSpan.FromSeconds(30);
        private string _keyPrefix;
        private ILogger _logger;
        private IDatabase _redisDatabase;
        private RedisOptions _redisOptions;
        private RedisProvider _redisProvider;
        private TimeSpan _timeout = TimeSpan.FromSeconds(300);

        #endregion Private Variables

        #region Public Methods

        public void AddLockKey(string lockKey)
        {
            throw new NotImplementedException("There is no need to add lock key when use redis lock service.");
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            using (var redisLock = RedLock.Acquire(_redisDatabase, GetRedisKey(lockKey), _timeout, _expiries))
            {
                action();
            }
        }

        public async Task ExecuteInLockAsync(string lockKey, Action action)
        {
            var redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(lockKey), _timeout, _expiries);
            try
            {
                action();
            }
            finally
            {
                await redisLock.DisposeAsync();
            }
        }

        public async Task ExecuteInLockAsync(string lockKey, Action<object> action, object state)
        {
            var redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(lockKey), _timeout, _expiries);
            try
            {
                action(state);
            }
            finally
            {
                await redisLock.DisposeAsync();
            }
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<Task> action)
        {
            var redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(lockKey), _timeout, _expiries);
            try
            {
                await action();
            }
            finally
            {
                await redisLock.DisposeAsync();
            }
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<object, Task<object>> action, object state)
        {
            var redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(lockKey), _timeout, _expiries);
            try
            {
                await action(state);
            }
            finally
            {
                await redisLock.DisposeAsync();
            }
        }

        public RedLockService Initialize(
            RedisOptions redisOptions,
            string keyPrefix = "enode",
            TimeSpan? timeout = null,
            TimeSpan? expiries = null
            )
        {
            _redisOptions = redisOptions;
            _keyPrefix = keyPrefix;

            Ensure.NotNull(_redisOptions, "redisOptions");
            Ensure.NotNull(_redisOptions.ConnectionString, "redisOptions.ConnectionString");
            Ensure.Positive(_redisOptions.DatabaseId, "redisOptions.DatabaseId");
            Ensure.NotNull(_keyPrefix, "keyPrefix");

            if (timeout.HasValue)
            {
                _timeout = timeout.Value;
            }

            if (expiries.HasValue)
            {
                _expiries = expiries.Value;
            }

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _redisProvider = new RedisProvider(_redisOptions);

            _redisDatabase = _redisProvider.GetDatabase();

            return this;
        }

        #endregion Public Methods

        #region Private Methods

        private RedisKey GetRedisKey(string key)
        {
            return $"{_keyPrefix}:lock:{key}";
        }

        #endregion Private Methods
    }
}