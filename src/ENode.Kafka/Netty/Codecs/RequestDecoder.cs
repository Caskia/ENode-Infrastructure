using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Transport.Channels;
using ENode.Kafka.Utils;
using System;
using System.Collections.Generic;
using System.IO;

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

            Stream inputStream = null;
            try
            {
                inputStream = new ReadOnlyByteBufferStream(message, false);
                var inputMemoryStream = new MemoryStream();
                inputStream.CopyTo(inputMemoryStream);

                var decoded = ObjectUtils.DeserializeFromStream(inputMemoryStream);

                output.Add(decoded);
            }
            catch (Exception exception)
            {
                throw new CodecException(exception);
            }
            finally
            {
                inputStream?.Dispose();
            }
        }
    }
}