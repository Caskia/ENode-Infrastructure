using DotNetty.Transport.Channels;
using ECommon.Components;
using ECommon.Logging;
using ENode.Kafka.Netty;
using System;
using System.Text;

namespace ENode.Kafka.Tests.Netty
{
    [Component]
    public class ServerHandler : ChannelHandlerAdapter
    {
        private readonly ILogger _logger;
        private readonly ServerMessageBox _messageBox;

        public ServerHandler(
            ServerMessageBox messageBox
            )
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name); ;
            _messageBox = messageBox;
        }

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            if (message != null)
            {
                var request = message as Request;
                _messageBox.AddAsync(request).Wait();

                _logger.Info("Received from client: " + Encoding.UTF8.GetString(request.Body));
            }
        }

        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            _logger.Error("Exception: " + exception, exception);
            context.CloseAsync();
        }
    }
}