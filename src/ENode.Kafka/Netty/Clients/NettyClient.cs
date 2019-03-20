using DotNetty.Codecs;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using ECommon.Components;
using ECommon.Logging;
using ENode.Kafka.Netty.Codecs;
using System;
using System.Net;
using System.Threading.Tasks;

namespace ENode.Kafka.Netty
{
    public class NettyClient
    {
        private readonly ILogger _logger;
        private readonly IPEndPoint _serverEndPoint;
        private readonly NettyClientSetting _setting;
        private Bootstrap _bootstrap;
        private MultithreadEventLoopGroup _group;

        #region Public Properties

        public IChannel Channel { get; private set; }

        #endregion Public Properties

        public NettyClient(IPEndPoint serverEndPoint, NettyClientSetting setting)
        {
            _serverEndPoint = serverEndPoint;

            _setting = setting ?? new NettyClientSetting(channel =>
            {
                var pipeline = channel.Pipeline;

                pipeline.AddLast(typeof(LengthFieldPrepender).Name, new LengthFieldPrepender(2));
                pipeline.AddLast(typeof(LengthFieldBasedFrameDecoder).Name, new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                pipeline.AddLast(typeof(RequestEncoder).Name, new RequestEncoder());
                pipeline.AddLast(typeof(RequestDecoder).Name, new RequestDecoder());
            });
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().Name);

            InitializeNetty();
        }

        public async Task<NettyClient> ShutdownAsync()
        {
            await ShutdownNettyClientAsync();
            return this;
        }

        public async Task<NettyClient> StartAsync()
        {
            await StartNettyClientAsync();
            return this;
        }

        private void InitializeNetty()
        {
            _group = new MultithreadEventLoopGroup();

            _bootstrap =
                    new Bootstrap()
                   .Group(_group)
                   .Channel<TcpSocketChannel>()
                   .Option(ChannelOption.TcpNodelay, true)
                   .Handler(new ActionChannelInitializer<ISocketChannel>(_setting.ChannelAction));
        }

        private async Task ShutdownGroupAsync()
        {
            if (_group == null)
            {
                throw new NotImplementedException("Need initialize netty client");
            }

            await _group.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1));
        }

        private async Task ShutdownNettyClientAsync()
        {
            await Channel.CloseAsync();
            await ShutdownGroupAsync();
        }

        private async Task StartNettyClientAsync()
        {
            try
            {
                Channel = await _bootstrap.ConnectAsync(_serverEndPoint);
            }
            catch
            {
                await ShutdownGroupAsync();
            }
        }
    }
}