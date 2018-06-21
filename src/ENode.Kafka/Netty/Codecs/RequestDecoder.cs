using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using ENode.Kafka.Extensions;
using System;
using System.Collections.Generic;

namespace ENode.Kafka.Netty.Codecs
{
    public class RequestDecoder : ByteToMessageDecoder
    {
        protected override void Decode(IChannelHandlerContext context, IByteBuffer message, List<object> output)
        {
            int length = message.ReadableBytes;
            if (length <= 0)
            {
                return;
            }

            try
            {
                var bytes = new byte[message.ReadableBytes];
                message.ReadBytes(bytes);

                var decoded = bytes.ToObject();

                output.Add(decoded);
            }
            catch (Exception exception)
            {
                throw new CodecException(exception);
            }
        }
    }
}