using DotNetty.Codecs;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Libuv;
using ECommon.Components;
using ECommon.Logging;
using ENode.Kafka.Netty.Codecs;
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
        private WorkerEventLoopGroup _workerGroup;

        #region Public Properties

        public IChannel Channel { get; private set; }

        #endregion Public Properties

        public NettyServer(IPEndPoint listeningEndPoint, NettyServerSetting setting = null) : this("Netty server", listeningEndPoint, setting)
        {
        }

        public NettyServer(string name, IPEndPoint listeningEndPoint, NettyServerSetting setting)
        {
            _listeningEndPoint = listeningEndPoint;

            _setting = setting ?? new NettyServerSetting(channel =>
            {
                var pipeline = channel.Pipeline;

                pipeline.AddLast(typeof(LengthFieldPrepender).Name, new LengthFieldPrepender(2));
                pipeline.AddLast(typeof(LengthFieldBasedFrameDecoder).Name, new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                pipeline.AddLast(typeof(RequestEncoder).Name, new RequestEncoder());
                pipeline.AddLast(typeof(RequestDecoder).Name, new RequestDecoder(Request.Parser));
            });
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
                   .ChildHandler(new ActionChannelInitializer<IChannel>(_setting.ChannelAction));
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
            await Channel.CloseAsync();
            await ShutdownGroupAsync();
        }

        private async Task StartNettyServerAsync()
        {
            try
            {
                Channel = await _bootstrap.BindAsync(_listeningEndPoint);
            }
            catch
            {
                await ShutdownGroupAsync();
            }
        }
    }
}