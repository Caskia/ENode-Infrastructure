using System;

namespace ENode.Kafka.Netty
{
    [Serializable]
    public class Request
    {
        public byte[] Body { get; set; }

        public short Code { get; set; }
    }
}