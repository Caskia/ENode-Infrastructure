using System;
using System.Threading.Tasks;

namespace ENode.Lock.Redis
{
    public class WorkContext
    {
        public Func<Task> Action { get; set; }

        public DateTime ExpirationTime { get; set; }

        public bool IsRunning { get; set; } = false;

        public string LockKey { get; set; }

        public TaskCompletionSource<bool> TaskCompletionSource { get; set; }
    }
}