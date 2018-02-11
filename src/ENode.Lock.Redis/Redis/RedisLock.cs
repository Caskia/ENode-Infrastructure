﻿using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace ENode.Lock.Redis
{
    public class RedisLock : IDisposable
    {
        #region Private Variables

        private static readonly TimeSpan DefaultHoldDuration = TimeSpan.FromMilliseconds(25);
        private static readonly string OwnerId = Guid.NewGuid().ToString();

        private static AsyncLocal<ISet<RedisKey>> _heldLocks = new AsyncLocal<ISet<RedisKey>>();

        private readonly bool _holdsLock;

        private readonly RedisKey _key;

        private readonly IDatabase _redis;

        private readonly Timer _slidingExpirationTimer;

        private volatile bool _isDisposed = false;

        #endregion Private Variables

        #region Ctor

        private RedisLock(IDatabase redis, RedisKey key, bool holdsLock, TimeSpan holdDuration)
        {
            _redis = redis;
            _key = key;
            _holdsLock = holdsLock;

            if (holdsLock)
            {
                HeldLocks.Add(_key);

                // start sliding expiration timer at half timeout intervals
                var halfLockHoldDuration = TimeSpan.FromTicks(holdDuration.Ticks / 2);
                _slidingExpirationTimer = new Timer(ExpirationTimerTick, holdDuration, halfLockHoldDuration, halfLockHoldDuration);
            }
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

            if (HeldLocks.Contains(key))
            {
                // lock is already held
                return new RedisLock(redis, key, false, holdDuration);
            }

            // The comparison below uses timeOut as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeOut;
            do
            {
                if (redis.LockTake(key, OwnerId, holdDuration))
                {
                    // we have successfully acquired the lock
                    return new RedisLock(redis, key, true, holdDuration);
                }

                SleepBackOffMultiplier(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeOut})");
        }

        public void Dispose()
        {
            if (_holdsLock)
            {
                _isDisposed = true;
                _slidingExpirationTimer.Dispose();

                if (!_redis.LockRelease(_key, OwnerId))
                {
                    Debug.WriteLine("Lock {0} already timed out", _key);
                }

                HeldLocks.Remove(_key);
            }
        }

        #endregion Public Methods

        #region Private Methods

        private static ISet<RedisKey> HeldLocks
        {
            get
            {
                var value = _heldLocks.Value;
                if (value == null)
                    _heldLocks.Value = value = new HashSet<RedisKey>();
                return value;
            }
        }

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

        private void ExpirationTimerTick(object state)
        {
            if (!_isDisposed)
            {
                _redis.LockExtend(_key, OwnerId, (TimeSpan)state);
            }
        }

        #endregion Private Methods
    }
}