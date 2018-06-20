using DotNetty.Transport.Channels;
using System;

namespace ENode.Kafka.Netty
{
    public class NettyClientSetting
    {
        public NettyClientSetting(Action<IChannel> channelAction)
        {
            ChannelAction = channelAction ?? throw new ArgumentNullException(nameof(channelAction));
        }

        public Action<IChannel> ChannelAction { get; set; }
    }
}