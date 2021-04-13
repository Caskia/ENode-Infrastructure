using DotNetty.Codecs.Protobuf;
using Google.Protobuf;

namespace ENode.Kafka.Netty.Codecs
{
    public class RequestDecoder : ProtobufDecoder
    {
        public RequestDecoder(MessageParser messageParser) : base(messageParser)
        {
        }
    }
}