using System.Collections.Generic;

namespace ENode.Kafka.Netty
{
    public class NettyServerSetting
    {
        public IList<ChannelHandlerInstance> ChannelHandlerInstances { get; set; }
    }
}