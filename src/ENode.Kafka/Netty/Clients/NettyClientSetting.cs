using DotNetty.Transport.Channels;
using System.Collections.Generic;

namespace ENode.Kafka.Netty
{
    public class NettyClientSetting
    {
        public IList<IChannelHandler> ChannelHandlers { get; set; }
    }
}