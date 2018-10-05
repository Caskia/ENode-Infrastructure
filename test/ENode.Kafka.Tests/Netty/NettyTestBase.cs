using ECommon.Configurations;
using ENode.Configurations;
using System.Reflection;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace ENode.Kafka.Tests.Netty
{
    public class NettyTestBase
    {
        public NettyTestBase()
        {
            var assemblies = new[] { Assembly.GetExecutingAssembly() };

            var enode = ECommonConfiguration.Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .CreateENode(new ConfigurationSetting())
                .RegisterBusinessComponents(assemblies)
                .RegisterENodeComponents();

            enode.GetCommonConfiguration()
              .BuildContainer();
        }
    }
}