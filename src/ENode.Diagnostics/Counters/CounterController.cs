using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ENode.Diagnostics.Configurations;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace ENode.Diagnostics.Counters
{
    public class CounterController
    {
        private readonly IBinarySerializer _binarySerializer;
        private readonly ICounterConfiguration _counterConfiguration;
        private readonly SocketRemotingServer _counterSocketRemotingServer;
        private readonly SocketRemotingClient _diagnosticsSocketRemotingClient;
        private readonly ILogger _logger;
        private readonly IScheduleService _scheduleService;

        public CounterController(
            IBinarySerializer binarySerializer,
            ICounterConfiguration counterConfiguration,
            IDiagnosticsServerConfiguration diagnosticsServerConfiguration,
            ILoggerFactory loggerFactory,
            IScheduleService scheduleService
            )
        {
            _binarySerializer = binarySerializer;
            _counterConfiguration = counterConfiguration;
            _logger = loggerFactory.Create(GetType().FullName);
            _scheduleService = scheduleService;

            var counterName = "ENode.Counter.RemotingServer";
            if (!string.IsNullOrEmpty(_counterSocketRemotingServer.Name))
            {
                counterName = _counterSocketRemotingServer.Name;
            }
            _counterSocketRemotingServer = new SocketRemotingServer(counterName, _counterConfiguration.Address);
        }
    }
}