using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using StackExchange.Redis;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace ENode.Lock.Redis
{
    public class RedLockQueueService : ILockService
    {
        #region Private Variables

        private TimeSpan _expiries = TimeSpan.FromSeconds(30);
        private string _keyPrefix;
        private ConcurrentDictionary<string, BufferBlock<WorkContext>> _lockQueues = new ConcurrentDictionary<string, BufferBlock<WorkContext>>();
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

        public async Task ExecuteInLockAsync(string lockKey, Action action)
        {
            var context = new WorkContext()
            {
                LockKey = lockKey,
                TaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
                ExpirationTime = DateTime.UtcNow + _timeout,
                Action = () =>
                {
                    action();
                    return Task.CompletedTask;
                },
            };

            GetOrCreateExecutingQueue(lockKey).Post(context);

            await WaitWorkContextResultAsync(context);
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<Task> action)
        {
            var context = new WorkContext()
            {
                LockKey = lockKey,
                TaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
                ExpirationTime = DateTime.UtcNow + _timeout,
                Action = action
            };

            GetOrCreateExecutingQueue(lockKey).Post(context);

            await WaitWorkContextResultAsync(context);
        }

        public async Task ExecuteInLockAsync(string lockKey, Action<object> action, object state)
        {
            var context = new WorkContext()
            {
                LockKey = lockKey,
                TaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
                ExpirationTime = DateTime.UtcNow + _timeout,
                Action = () =>
                {
                    action(state);
                    return Task.CompletedTask;
                }
            };

            GetOrCreateExecutingQueue(lockKey).Post(context);

            await WaitWorkContextResultAsync(context);
        }

        public async Task ExecuteInLockAsync(string lockKey, Func<object, Task<object>> action, object state)
        {
            var context = new WorkContext()
            {
                LockKey = lockKey,
                TaskCompletionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously),
                ExpirationTime = DateTime.UtcNow + _timeout,
                Action = async () =>
                {
                    await action(state);
                }
            };

            GetOrCreateExecutingQueue(lockKey).Post(context);

            await WaitWorkContextResultAsync(context);
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

        private BufferBlock<WorkContext> GetOrCreateExecutingQueue(string lockKey)
        {
            var executingQueue = new BufferBlock<WorkContext>();
            if (_lockQueues.TryGetValue(lockKey, out BufferBlock<WorkContext> queue))
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
                    if (_lockQueues.TryGetValue(lockKey, out BufferBlock<WorkContext> addedQueue))
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

        private async Task ProcessQueueTaskAsync(BufferBlock<WorkContext> queue)
        {
            while (await queue.OutputAvailableAsync())
            {
                var context = await queue.ReceiveAsync();

                lock (context)
                {
                    context.IsRunning = true;
                    if (context.IsTimeout)
                    {
                        context.TaskCompletionSource.TrySetException(new DistributedLockTimeoutException($"Failed to acquire lock on {context.LockKey} within given timeout ({_timeout})"));
                        continue;
                    }
                }

                var redisLock = default(RedLock);
                try
                {
                    redisLock = await RedLock.AcquireAsync(_redisDatabase, GetRedisKey(context.LockKey), context.ExpirationTime - DateTime.UtcNow, _expiries);
                    await context.Action();
                }
                catch (Exception ex)
                {
                    context.TaskCompletionSource.TrySetException(ex);
                }
                finally
                {
                    context.TaskCompletionSource.TrySetResult(true);

                    if (redisLock != null)
                    {
                        try
                        {
                            await redisLock.DisposeAsync();
                        }
                        catch
                        {
                        }
                    }
                }
            };
        }

        private async Task WaitWorkContextResultAsync(WorkContext context)
        {
            var timeoutTask = Task.Delay(_timeout);
            if (await Task.WhenAny(context.TaskCompletionSource.Task, timeoutTask) == timeoutTask)
            {
                lock (context)
                {
                    if (!context.IsRunning)
                    {
                        context.IsTimeout = true;
                    }
                }
            }

            await context.TaskCompletionSource.Task;
        }

        #endregion Private Methods
    }
}