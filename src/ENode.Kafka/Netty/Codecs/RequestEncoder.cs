using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using ENode.Kafka.Extensions;

namespace ENode.Kafka.Netty.Codecs
{
    public class RequestEncoder : MessageToByteEncoder<Request>
    {
        protected override void Encode(IChannelHandlerContext context, Request message, IByteBuffer output)
        {
            if (message != null)
            {
                output.WriteBytes(message.ToByteArray());
            }
        }
    }
}