using ECommon.Components;
using ECommon.Logging;
using ENode.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ENode.Kafka.Scheduling
{
    public class AsyncWorker
    {
        private readonly Func<Task> _action;
        private readonly string _actionName;
        private readonly AsyncLock _asyncLock = new AsyncLock();
        private readonly ILogger _logger;
        private Status _status;

        /// <summary>Initialize a new worker with the specified action.
        /// </summary>
        /// <param name="actionName">The action name.</param>
        /// <param name="action">The action to run by the worker.</param>
        public AsyncWorker(string actionName, Func<Task> action)
        {
            _actionName = actionName;
            _action = action;
            _status = Status.Initial;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        private enum Status
        {
            Initial,
            Running,
            StopRequested
        }

        /// <summary>Returns the action name of the current worker.
        /// </summary>
        public string ActionName
        {
            get { return _actionName; }
        }

        /// <summary>Start the worker if it is not running.
        /// </summary>
        public async Task<AsyncWorker> StartAsync()
        {
            using (await _asyncLock.LockAsync().ConfigureAwait(false))
            {
                if (_status == Status.Running) return this;

                _status = Status.Running;
                await Task.Factory.StartNew(async () => await LoopAsync(this), TaskCreationOptions.LongRunning);
            }

            return this;
        }

        /// <summary>Request to stop the worker.
        /// </summary>
        public async Task<AsyncWorker> StopAsync()
        {
            using (await _asyncLock.LockAsync().ConfigureAwait(false))
            {
                if (_status == Status.StopRequested) return this;

                _status = Status.StopRequested;

                return this;
            }
        }

        private async Task LoopAsync(object data)
        {
            var worker = (AsyncWorker)data;

            while (worker._status == Status.Running)
            {
                try
                {
                    await _action();
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Async Worker has exception, actionName:{0}", _actionName), ex);
                }
            }
        }
    }
}