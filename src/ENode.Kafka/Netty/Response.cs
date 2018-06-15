namespace ENode.Kafka.Netty
{
    public class Response
    {
        public short RequestCode { get; set; }

        public byte[] ResponseBody { get; set; }

        public short ResponseCode { get; set; }
    }
}