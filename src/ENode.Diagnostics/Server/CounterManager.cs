using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using ENode.Diagnostics.Protocols.Servers.Requests;
using ENode.Infrastructure;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

namespace ENode.Diagnostics.Server
{
    public class CounterManager
    {
        #region Fields

        private readonly AsyncLock _asyncLock = new AsyncLock();
        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentDictionary<string, Counter> _counterDict;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly IScheduleService _scheduleService;

        #endregion Fields

        #region Ctor

        public CounterManager()
        {
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _counterDict = new ConcurrentDictionary<string, Counter>();
        }

        #endregion Ctor

        #region Methods

        public void RegisterCounter(ITcpConnection connection, CounterRegistrationRequest request)
        {
            using (_asyncLock.Lock())
            {
                var counterName = GetCounterName(request.ProcessName, connection);
                var connectionId = connection.RemotingEndPoint.ToAddress();

                _counterDict.AddOrUpdate(
                    counterName,
                    new Counter()
                    {
                        Connection = connection,
                        ConnectionId = connectionId,
                        FirstRegisteredTime = DateTime.UtcNow,
                        ServiceAddress = $"{(connection.RemotingEndPoint as IPEndPoint).Address}:{request.ServicePort}",
                        LastActiveTime = DateTime.UtcNow,
                        Name = counterName,
                        ProcessId = request.ProcessId,
                        ProcessName = request.ProcessName
                    },
                    (key, c) =>
                    {
                        c.LastActiveTime = DateTime.UtcNow;
                        return c;
                    });
            }
        }

        public void UnregisterCounter(ITcpConnection connection, CounterUnregistrationRequest request)
        {
            using (_asyncLock.Lock())
            {
                var counterName = GetCounterName(request.ProcessName, connection);

                if (_counterDict.TryRemove(counterName, out Counter removed))
                {
                    _logger.InfoFormat("Unregistered counter, counter info: {0}", _jsonSerializer.Serialize(removed));
                }
            }
        }

        #endregion Methods

        private string GetCounterName(string processName, ITcpConnection connection)
        {
            var remotingIPEndPoint = connection.RemotingEndPoint as IPEndPoint;
            if (remotingIPEndPoint == null)
            {
                return connection.Id.ToString();
            }

            return $"{processName}@{remotingIPEndPoint.Address}:{remotingIPEndPoint.Port}";
        }

        private void ScanNotActiveCounter()
        {
            using (_asyncLock.Lock())
            {
                var notActiveCounters = new List<Counter>();
                foreach (var counter in _counterDict.Values)
                {
                    //ToDo: timeout settable
                    if (counter.IsTimeout(10000))
                    {
                        notActiveCounters.Add(counter);
                    }
                }

                foreach (var counter in notActiveCounters)
                {
                    if (_counterDict.TryRemove(counter.Name, out Counter removed))
                    {
                        _logger.InfoFormat("Removed timeout counter, counter info: {0}, lastActiveTime: {1}", _jsonSerializer.Serialize(removed), removed.LastActiveTime);
                    }
                }
            }
        }

        private class Counter
        {
            public ITcpConnection Connection { get; set; }

            public string ConnectionId { get; set; }

            public DateTime FirstRegisteredTime { get; set; }

            public DateTime LastActiveTime { get; set; }

            public string Name { get; set; }

            public int ProcessId { get; set; }

            public string ProcessName { get; set; }

            public string ServiceAddress { get; set; }

            public bool IsTimeout(double timeoutMilliseconds)
            {
                return (DateTime.Now - LastActiveTime).TotalMilliseconds >= timeoutMilliseconds;
            }
        }
    }
}