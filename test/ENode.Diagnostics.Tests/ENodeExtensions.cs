using ENode.Configurations;

namespace ENode.Diagnostics.Tests
{
    public static class ENodeExtensions
    {
        public static ENodeConfiguration BuildContainer(this ENodeConfiguration enodeConfiguration)
        {
            enodeConfiguration.GetCommonConfiguration().BuildContainer();
            return enodeConfiguration;
        }
    }
}