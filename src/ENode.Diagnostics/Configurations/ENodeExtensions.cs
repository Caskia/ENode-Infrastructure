using ENode.Configurations;
using ENode.Diagnostics.Commanding;
using ENode.Diagnostics.Eventing;

namespace ENode.Diagnostics
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Use DiagnosticsService.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseENodeDiagnosticsService(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ICommandMailboxCounter, CommandMailboxCounter>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventMailboxCounter, EventMailboxCounter>();

            return eNodeConfiguration;
        }
    }
}