using System;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public interface ILockService : ENode.Infrastructure.ILockService
    {
        Task ExecuteInLockAsync(string lockKey, Action action);

        //Task ExecuteInLockAsync(string lockKey, Func<Task> action);

        //Task ExecuteInLockAsync(string lockKey, Action<object> action, object state);

        //Task ExecuteInLockAsync(string lockKey, Func<object, Task<object>> action, object state);
    }
}