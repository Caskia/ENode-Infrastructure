using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class RedLock : IDisposable
    {
        #region Private Variables

        private static readonly TimeSpan DefaultExpiries = TimeSpan.FromMilliseconds(30);
        private static readonly string OwnerId = Guid.NewGuid().ToString();

        private readonly Timer _keepLockAliveTimer;
        private readonly RedisKey _key;
        private readonly IDatabase _redis;
        private volatile bool _isDisposed = false;

        #endregion Private Variables

        #region Ctor

        private RedLock(IDatabase redis, RedisKey key, TimeSpan expiries)
        {
            _redis = redis;
            _key = key;

            // start sliding expiration timer at half timeout intervals
            var halfExpiries = TimeSpan.FromTicks(expiries.Ticks / 2);
            _keepLockAliveTimer = new Timer(KeepLockAlive, expiries, halfExpiries, halfExpiries);
        }

        #endregion Ctor

        #region Public Methods

        public static IDisposable Acquire(IDatabase redis, RedisKey key, TimeSpan timeout)
        {
            return Acquire(redis, key, timeout, DefaultExpiries);
        }

        public static IDisposable Acquire(IDatabase redis, RedisKey key, TimeSpan timeout, TimeSpan expiries)
        {
            if (redis == null)
                throw new ArgumentNullException(nameof(redis));

            // The comparison below uses timeout as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeout;
            do
            {
                if (redis.LockTake(key, OwnerId, expiries))
                {
                    // we have successfully acquired the lock
                    return new RedLock(redis, key, expiries);
                }

                SleepBackOffMultiplier(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeout})");
        }

        public static Task<RedLock> AcquireAsync(IDatabase redis, RedisKey key, TimeSpan timeout)
        {
            return AcquireAsync(redis, key, timeout, DefaultExpiries);
        }

        public static async Task<RedLock> AcquireAsync(IDatabase redis, RedisKey key, TimeSpan timeout, TimeSpan expiries)
        {
            if (redis == null)
                throw new ArgumentNullException(nameof(redis));

            // The comparison below uses timeout as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeout;
            do
            {
                if (await redis.LockTakeAsync(key, OwnerId, expiries))
                {
                    //we have successfully acquired the lock
                    return new RedLock(redis, key, expiries);
                }

                await SleepBackOffMultiplierAsync(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeout})");
        }

        public void Dispose()
        {
            _isDisposed = true;
            _keepLockAliveTimer.Dispose();

            if (!_redis.LockRelease(_key, OwnerId))
            {
                //Debug.WriteLine($"Lock {_key} already timed out");
            }
        }

        public async Task DisposeAsync()
        {
            _isDisposed = true;
            _keepLockAliveTimer.Dispose();

            if (!await _redis.LockReleaseAsync(_key, OwnerId))
            {
                //Debug.WriteLine($"Lock {_key} already timed out");
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

        private void KeepLockAlive(object state)
        {
            if (!_isDisposed)
            {
                _redis.LockExtendAsync(_key, OwnerId, (TimeSpan)state);

                //Debug.WriteLine($"Lock {_key} extend lock time");
            }
        }

        #endregion Private Methods
    }
}