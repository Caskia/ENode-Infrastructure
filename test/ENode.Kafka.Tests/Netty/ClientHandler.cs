using DotNetty.Transport.Channels;
using ECommon.Components;
using ECommon.Logging;
using ENode.Kafka.Netty;
using System;

namespace ENode.Kafka.Tests.Netty
{
    [Component]
    public class ClientHandler : ChannelHandlerAdapter
    {
        private readonly ILogger _logger;
        private readonly ClientMessageBox _messageBox;

        public ClientHandler(
            ClientMessageBox messageBox
            )
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name); ;
            _messageBox = messageBox;
        }

        //public override void ChannelActive(IChannelHandlerContext context)
        //    => context.WriteAndFlushAsync(new Request() { Code = 1, Body = Encoding.UTF8.GetBytes("ping") });

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            if (message == null)
            {
                _logger.Info("message content is null.");
                return;
            }

            if (message is Request request)
            {
                _messageBox.AddAsync(request).Wait();

                _logger.Info("Received from server: " + request.Body.ToStringUtf8());
            }
            else
            {
                _logger.Warn($"message type[{message.GetType().FullName}] not match Request[{typeof(Request).FullName}]");
            }
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            _logger.Error("Exception: " + exception, exception);
            context.CloseAsync();
        }
    }
}