using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class RedLockQueueService : ILockService
    {
        #region Private Variables

        private TimeSpan _expiries = TimeSpan.FromSeconds(30);
        private string _keyPrefix;
        private ConcurrentDictionary<string, BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)>> _lockQueues = new ConcurrentDictionary<string, BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)>>();
        private ILogger _logger;
        private IDatabase _redisDatabase;
        private RedisOptions _redisOptions;
        private RedisProvider _redisProvider;
        private TimeSpan _timeout = TimeSpan.FromSeconds(300);

        #endregion Private Variables

        #region Ctor

        public RedLockQueueService()
        {
        }

        #endregion Ctor

        #region Public Methods

        public void AddLockKey(string lockKey)
        {
            throw new NotImplementedException("There is no need to add lock key when use redis lock service.");
        }

        public void ExecuteInLock(string lockKey, Action action)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            GetOrCreateExecutingQueue(lockKey).Add((
                lockKey,
                tcs,
                DateTime.UtcNow + _timeout,
                () =>
                {
                    action();
                    return Task.CompletedTask;
                }
            ));

            tcs.Task.Wait();
        }

        public async Task ExecuteInLockAsync(string lockKey, Action action)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            GetOrCreateExecutingQueue(lockKey).Add((
                lockKey,
                tcs,
                DateTime.UtcNow + _timeout,
                () =>
                {
                    action();
                    return Task.CompletedTask;
                }
            ));

            await tcs.Task;
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<Task> action)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            GetOrCreateExecutingQueue(lockKey).Add((
                lockKey,
                tcs,
                DateTime.UtcNow + _timeout,
                async () =>
                {
                    await action();
                }
            ));

            await tcs.Task;
        }

        public async Task ExecuteInLockAsync(string lockKey, Action<object> action, object state)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            GetOrCreateExecutingQueue(lockKey).Add((
                lockKey,
                tcs,
                DateTime.UtcNow + _timeout,
                () =>
                {
                    action(state);
                    return Task.CompletedTask;
                }
            ));

            await tcs.Task;
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<object, Task<object>> action, object state)
        {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            GetOrCreateExecutingQueue(lockKey).Add((
                lockKey,
                tcs,
                DateTime.UtcNow + _timeout,
                async () =>
                {
                    await action(state);
                }
            ));

            await tcs.Task;
        }

        public RedLockQueueService Initialize(
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

        private BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)> GetOrCreateExecutingQueue(string lockKey)
        {
            var executingQueue = new BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)>();
            if (_lockQueues.TryGetValue(lockKey, out BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)> queue))
            {
                executingQueue = queue;
            }
            else
            {
                if (_lockQueues.TryAdd(lockKey, executingQueue))
                {
                    Task.Factory.StartNew(async () => await ProcessQueueTaskAsync(executingQueue));
                }
                else
                {
                    if (_lockQueues.TryGetValue(lockKey, out BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)> addedQueue))
                    {
                        executingQueue = addedQueue;
                    }
                }
            }

            return executingQueue;
        }

        private RedisKey GetRedisKey(string key)
        {
            return $"{_keyPrefix}:lock:{key}";
        }

        private async Task ProcessQueueTaskAsync(BlockingCollection<(string lockKey, TaskCompletionSource<bool> tcs, DateTime expirationTime, Func<Task> action)> queue)
        {
            foreach (var item in queue.GetConsumingEnumerable())
            {
                var leftTimeSpan = item.expirationTime - DateTime.UtcNow;

                if (leftTimeSpan <= TimeSpan.Zero)
                {
                    item.tcs.TrySetException(new DistributedLockTimeoutException($"Failed to acquire lock on {item.lockKey} within given timeout ({_timeout})"));
                    continue;
                }

                var redisLock = default(RedLock);
                try
                {
                    redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(item.lockKey), leftTimeSpan, _expiries);
                    await item.action();
                }
                catch (Exception ex)
                {
                    item.tcs.TrySetException(ex);
                }
                finally
                {
                    if (redisLock != null)
                    {
                        await redisLock.DisposeAsync();
                    }
                    item.tcs.TrySetResult(true);
                }
            }
        }

        #endregion Private Methods
    }
}