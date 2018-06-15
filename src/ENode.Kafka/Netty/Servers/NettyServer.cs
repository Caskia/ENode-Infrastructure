using DotNetty.Codecs;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Libuv;
using ECommon.Components;
using ECommon.Logging;
using System;
using System.Net;
using System.Threading.Tasks;

namespace ENode.Kafka.Netty
{
    public class NettyServer
    {
        private readonly IPEndPoint _listeningEndPoint;
        private readonly ILogger _logger;
        private readonly NettyServerSetting _setting;
        private ServerBootstrap _bootstrap;
        private DispatcherEventLoopGroup _bossGroup;
        private IChannel _channel;
        private WorkerEventLoopGroup _workerGroup;

        public NettyServer(IPEndPoint listeningEndPoint, NettyServerSetting setting = null) : this("Netty server", listeningEndPoint, setting)
        {
        }

        public NettyServer(string name, IPEndPoint listeningEndPoint, NettyServerSetting setting)
        {
            _listeningEndPoint = listeningEndPoint;

            _setting = setting ?? new NettyServerSetting();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(name ?? GetType().Name);

            InitializeNetty();
        }

        public NettyServer Shutdown()
        {
            ShutdownNettyServerAsync().Wait();
            return this;
        }

        public NettyServer Start()
        {
            StartNettyServerAsync().Wait();
            return this;
        }

        private void InitializeNetty()
        {
            var dispatcher = new DispatcherEventLoopGroup();
            _bossGroup = dispatcher;
            _workerGroup = new WorkerEventLoopGroup(dispatcher);

            _bootstrap = new ServerBootstrap()
                   .Group(_bossGroup, _workerGroup)
                   .Channel<TcpServerChannel>()
                   .Option(ChannelOption.SoBacklog, 100)
                   .ChildHandler(new ActionChannelInitializer<IChannel>(channel =>
                   {
                       var pipeline = channel.Pipeline;

                       pipeline.AddLast("string-encoder", new StringEncoder());
                       pipeline.AddLast("string-decoder", new StringDecoder());

                       if (_setting.ChannelHandlers != null && _setting.ChannelHandlers.Count > 0)
                       {
                           foreach (var channelHandler in _setting.ChannelHandlers)
                           {
                               pipeline.AddLast("echo", channelHandler);
                           }
                       }
                   })); ;
        }

        private async Task ShutdownGroupAsync()
        {
            if (_bossGroup == null || _workerGroup == null)
            {
                throw new NotImplementedException("Need initialize netty server");
            }

            await Task.WhenAll(
                    _bossGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1)),
                    _workerGroup.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1))
                    );
        }

        private async Task ShutdownNettyServerAsync()
        {
            await _channel.CloseAsync();
            await ShutdownGroupAsync();
        }

        private async Task StartNettyServerAsync()
        {
            InitializeNetty();
            try
            {
                _channel = await _bootstrap.BindAsync(_listeningEndPoint.Port);
            }
            catch
            {
                await ShutdownGroupAsync();
            }
        }
    }
}