﻿using DotNetty.Transport.Channels;
using ECommon.Components;
using ECommon.Logging;
using ENode.Kafka.Netty;
using System;

namespace ENode.Kafka
{
    public class CommandResultChannelHandler : ChannelHandlerAdapter
    {
        private readonly CommandResultProcessor _commandResultProcessor;
        private readonly ILogger _logger;

        public CommandResultChannelHandler(CommandResultProcessor commandResultProcessor)
        {
            _commandResultProcessor = commandResultProcessor;
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            if (message == null)
            {
                _logger.Info("message content is null.");
                return;
            }

            if (message is Request request)
            {
                _commandResultProcessor.HandleRequest(request);
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