using ENode.Lock.Redis;
using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace TestConsole
{
    public class QueueService
    {
        #region Private Variables

        private ConcurrentDictionary<string, BlockingCollection<WorkContext>> _lockQueues = new ConcurrentDictionary<string, BlockingCollection<WorkContext>>();

        private TimeSpan _timeout = TimeSpan.FromSeconds(300);

        #endregion Private Variables

        #region Ctor

        public QueueService()
        {
            //Task.Factory.StartNew(async () => await ProcessTaskAsync());
        }

        #endregion Ctor

        #region Public Methods

        public async Task ExecuteInLockAsync(string lockKey, Func<Task> action)
        {
            var context = new WorkContext()
            {
                LockKey = lockKey,
                TaskCompletionSource = new TaskCompletionSource<bool>(TaskContinuationOptions.RunContinuationsAsynchronously),
                ExpirationTime = DateTime.UtcNow + _timeout,
                Action = action
            };

            GetOrCreateExecutingQueue(lockKey).Add(context);

            await WaitWorkContextResultAsync(context);
        }

        #endregion Public Methods

        #region Private Methods

        private BlockingCollection<WorkContext> GetOrCreateExecutingQueue(string lockKey)
        {
            var executingQueue = new BlockingCollection<WorkContext>();
            if (_lockQueues.TryGetValue(lockKey, out BlockingCollection<WorkContext> queue))
            {
                executingQueue = queue;
            }
            else
            {
                if (_lockQueues.TryAdd(lockKey, executingQueue))
                {
                    Task.Factory.StartNew(async () => await ProcessQueueTaskAsync(executingQueue), TaskCreationOptions.RunContinuationsAsynchronously);
                }
                else
                {
                    if (_lockQueues.TryGetValue(lockKey, out BlockingCollection<WorkContext> addedQueue))
                    {
                        executingQueue = addedQueue;
                    }
                }
            }

            return executingQueue;
        }

        private async Task ProcessQueueTaskAsync(BlockingCollection<WorkContext> queue)
        {
            while (true)
            {
                if (queue.TryTake(out WorkContext context))
                {
                    Console.WriteLine($"try consume lock key queue{context.LockKey}'s task. ");

                    try
                    {
                        await context.Action();
                    }
                    catch (Exception ex)
                    {
                        context.TaskCompletionSource.TrySetException(ex);
                    }
                    finally
                    {
                        Console.WriteLine($"try set result consume lock key queue{context.LockKey}'s task. ");
                        context.TaskCompletionSource.TrySetResult(true);
                        Console.WriteLine($"complete set result consume lock key queue{context.LockKey}'s task. ");
                    }
                }
                else
                {
                    //await Task.Yield();
                    await Task.Delay(1);
                }
            }

            //foreach (var context in queue.GetConsumingEnumerable())
            //{
            //    Console.WriteLine($"try consume lock key queue{context.LockKey}'s task. ");

            //    try
            //    {
            //        await context.Action();
            //    }
            //    catch (Exception ex)
            //    {
            //        context.TaskCompletionSource.TrySetException(ex);
            //    }
            //    finally
            //    {
            //        Console.WriteLine($"try set result consume lock key queue{context.LockKey}'s task. ");
            //        context.TaskCompletionSource.TrySetResult(true);
            //        Console.WriteLine($"complete set result consume lock key queue{context.LockKey}'s task. ");
            //    }
            //}
        }

        private async Task ProcessTaskAsync()
        {
            while (true)
            {
                foreach (var queue in _lockQueues)
                {
                    while (queue.Value.TryTake(out WorkContext context))
                    {
                        try
                        {
                            await Task.Delay(100);
                            await context.Action();
                        }
                        catch (Exception ex)
                        {
                            context.TaskCompletionSource.TrySetException(ex);
                        }
                        finally
                        {
                            Console.WriteLine($"try set result consume lock key queue{context.LockKey}'s task.");
                            context.TaskCompletionSource.TrySetResult(true);
                            Console.WriteLine($"complete set result consume lock key queue{context.LockKey}'s task.");
                        }
                    }
                }
            }
        }

        private async Task WaitWorkContextResultAsync(WorkContext context)
        {
            await context.TaskCompletionSource.Task;
            Console.WriteLine($"final complete set result consume lock key queue{context.LockKey}'s task.");
        }

        #endregion Private Methods
    }
}