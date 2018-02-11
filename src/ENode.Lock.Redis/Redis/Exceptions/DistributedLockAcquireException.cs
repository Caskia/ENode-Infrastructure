using System;

namespace ENode.Lock.Redis.Exceptions
{
    public class DistributedLockAcquireException : Exception
    {
        public DistributedLockAcquireException(string resource)
            : base(
                $"Can not obtain a distributed lock on the '{resource}' resource."
                )
        {
            Resource = resource;
        }

        public string Resource { get; }
    }
}