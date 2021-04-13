using System;

namespace ENode.Kafka.Netty
{
    [Serializable]
    public class Request1
    {
        public byte[] Body { get; set; }

        public short Code { get; set; }
    }
}