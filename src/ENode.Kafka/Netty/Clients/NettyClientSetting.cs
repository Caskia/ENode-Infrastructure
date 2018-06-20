using System.Collections.Generic;

namespace ENode.Kafka.Netty
{
    public class NettyClientSetting
    {
        public IList<ChannelHandlerInstance> ChannelHandlerInstances { get; set; }
    }
}