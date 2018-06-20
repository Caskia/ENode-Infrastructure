using System;
using System.Collections.Generic;

namespace ENode.Kafka.Netty
{
    public class ChannelHandlerInstance
    {
        public List<object> Args { get; set; } = new List<object>();

        public Type Type { get; set; }
    }
}