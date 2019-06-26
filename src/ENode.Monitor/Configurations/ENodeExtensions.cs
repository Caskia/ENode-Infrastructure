using ENode.Configurations;
using ENode.Monitor.Commanding;
using ENode.Monitor.Eventing;

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
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ICommandMailboxMonitor, CommandMailboxMonitor>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventMailboxMonitor, EventMailboxMonitor>();

            return eNodeConfiguration;
        }
    }
}