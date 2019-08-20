using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class RedisLock : IDisposable
    {
        #region Private Variables

        private static readonly TimeSpan DefaultHoldDuration = TimeSpan.FromMilliseconds(30);
        private static readonly string OwnerId = Guid.NewGuid().ToString();
        private static AsyncLock Lock = new AsyncLock();

        private readonly RedisKey _key;

        private readonly IDatabase _redis;

        private readonly Timer _slidingExpirationTimer;

        private volatile bool _isDisposed = false;

        #endregion Private Variables

        #region Ctor

        private RedisLock(IDatabase redis, RedisKey key, TimeSpan holdDuration)
        {
            _redis = redis;
            _key = key;

            // start sliding expiration timer at half timeout intervals
            var halfLockHoldDuration = TimeSpan.FromTicks(holdDuration.Ticks / 2);
            _slidingExpirationTimer = new Timer(ExpirationTimerTick, holdDuration, halfLockHoldDuration, halfLockHoldDuration);
        }

        #endregion Ctor

        #region Public Methods

        public static IDisposable Acquire(IDatabase redis, RedisKey key, TimeSpan timeOut)
        {
            return Acquire(redis, key, timeOut, DefaultHoldDuration);
        }

        public static IDisposable Acquire(IDatabase redis, RedisKey key, TimeSpan timeOut, TimeSpan holdDuration)
        {
            if (redis == null)
                throw new ArgumentNullException(nameof(redis));

            // The comparison below uses timeOut as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeOut;
            do
            {
                if (redis.LockTake(key, OwnerId, holdDuration))
                {
                    // we have successfully acquired the lock
                    return new RedisLock(redis, key, holdDuration);
                }

                SleepBackOffMultiplier(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeOut})");
        }

        public static Task<RedisLock> AcquireAsync(IDatabase redis, RedisKey key, TimeSpan timeOut)
        {
            return AcquireAsync(redis, key, timeOut, DefaultHoldDuration);
        }

        public static async Task<RedisLock> AcquireAsync(IDatabase redis, RedisKey key, TimeSpan timeOut, TimeSpan holdDuration)
        {
            if (redis == null)
                throw new ArgumentNullException(nameof(redis));

            // The comparison below uses timeOut as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeOut;
            do
            {
                //using (var l = await Lock.LockAsync())
                //{
                if (await redis.LockTakeAsync(key, OwnerId, holdDuration))
                {
                    //we have successfully acquired the lock
                    return new RedisLock(redis, key, holdDuration);
                }
                //}

                await SleepBackOffMultiplierAsync(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeOut})");
        }

        public void Dispose()
        {
            _isDisposed = true;
            _slidingExpirationTimer.Dispose();

            if (!_redis.LockRelease(_key, OwnerId))
            {
                Debug.WriteLine($"Lock {_key} already timed out");
            }
        }

        public async Task DisposeAsync()
        {
            _isDisposed = true;
            _slidingExpirationTimer.Dispose();

            if (!await _redis.LockReleaseAsync(_key, OwnerId))
            {
                Debug.WriteLine($"Lock {_key} already timed out");
            }
        }

        #endregion Public Methods

        #region Private Methods

        private static void SleepBackOffMultiplier(int i, int maxWait)
        {
            if (maxWait <= 0) return;

            // exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            nextTry = Math.Min(nextTry, maxWait);

            Thread.Sleep(nextTry);
        }

        private static async Task SleepBackOffMultiplierAsync(int i, int maxWait)
        {
            if (maxWait <= 0) return;

            // exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            nextTry = Math.Min(nextTry, maxWait);

            await Task.Delay(nextTry);
        }

        private void ExpirationTimerTick(object state)
        {
            if (!_isDisposed)
            {
                _redis.LockExtendAsync(_key, OwnerId, (TimeSpan)state);

                Debug.WriteLine($"Lock {_key} extend lock time");
            }
        }

        #endregion Private Methods
    }
}