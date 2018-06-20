using DotNetty.Transport.Channels;
using System;

namespace ENode.Kafka.Netty
{
    public class NettyServerSetting
    {
        public NettyServerSetting(Action<IChannel> channelAction)
        {
            ChannelAction = channelAction ?? throw new ArgumentNullException(nameof(channelAction));
        }

        public Action<IChannel> ChannelAction { get; set; }
    }
}