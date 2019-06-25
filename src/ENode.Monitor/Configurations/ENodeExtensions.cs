using ENode.Configurations;
using ENode.Monitor.Commanding;
using System;
using System.Collections.Generic;
using System.Text;

namespace ENode.Monitor
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Use MonitorService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseENodeMonitorService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IProcessingCommandMailboxMonitor, ProcessingCommandMailboxMonitor>();
            return eNodeConfiguration;
        }
    }
}