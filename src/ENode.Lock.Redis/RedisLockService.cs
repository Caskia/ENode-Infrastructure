using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class RedisLockService : ILockService
    {
        #region Private Variables

        private TimeSpan _holdDurationTimeSpan = TimeSpan.FromSeconds(30);
        private string _keyPrefix;
        private ILogger _logger;
        private IDatabase _redisDatabase;
        private RedisOptions _redisOptions;
        private RedisProvider _redisProvider;
        private TimeSpan _timeOutTimeSpan = TimeSpan.FromSeconds(30);

        #endregion Private Variables

        #region Public Methods

        public void AddLockKey(string lockKey)
        {
            throw new NotImplementedException("There is no need to add lock key when use redis lock service.");
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            using (var redisLock = RedisLock.Acquire(_redisDatabase, GetRedisKey(lockKey), _timeOutTimeSpan, _holdDurationTimeSpan))
            {
                action();
            }
        }

        public async Task ExecuteInLockAsync(string lockKey, Action action)
        {
            var redisLock = await RedisLock.AcquireAsync(_redisDatabase, GetRedisKey(lockKey), _timeOutTimeSpan, _holdDurationTimeSpan);
            try
            {
                action();
            }
            finally
            {
                await redisLock.DisposeAsync();
            }
        }

        public RedisLockService Initialize(
            RedisOptions redisOptions,
            string keyPrefix = "default",
            TimeSpan? timeOut = null,
            TimeSpan? holdDuration = null
            )
        {
            _redisOptions = redisOptions;
            _keyPrefix = keyPrefix;

            Ensure.NotNull(_redisOptions, "redisOptions");
            Ensure.NotNull(_redisOptions.ConnectionString, "redisOptions.ConnectionString");
            Ensure.Positive(_redisOptions.DatabaseId, "redisOptions.DatabaseId");
            Ensure.NotNull(_keyPrefix, "keyPrefix");

            if (timeOut.HasValue)
            {
                _timeOutTimeSpan = timeOut.Value;
            }

            if (holdDuration.HasValue)
            {
                _holdDurationTimeSpan = holdDuration.Value;
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
            return $"enode:lock:{_keyPrefix}:{key}";
        }

        #endregion Private Methods
    }
}