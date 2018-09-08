using ECommon.Components;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Utilities;
using ENode.Kafka.Netty;
using ENode.Kafka.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class SendReplyService
    {
        private readonly IOHelper _ioHelper;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly string _name;
        private readonly ConcurrentDictionary<string, NettyClient> _remotingClientDict;
        private readonly string _scanInactiveCommandRemotingClientTaskName;
        private readonly IScheduleService _scheduleService;
        private readonly Object lockObject = new object();

        public SendReplyService(string name)
        {
            _name = name;
            _remotingClientDict = new ConcurrentDictionary<string, NettyClient>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _ioHelper = ObjectContainer.Resolve<IOHelper>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scanInactiveCommandRemotingClientTaskName = "ScanInactiveCommandRemotingClient_" + DateTime.Now.Ticks + new Random().Next(10000);
        }

        public void SendReply(short replyType, object replyData, string replyAddress)
        {
            Task.Factory.StartNew(obj =>
            {
                var context = obj as SendReplyContext;
                try
                {
                    var remotingClient = GetRemotingClient(context.ReplyAddress);
                    if (remotingClient == null) return;

                    if (!remotingClient.Channel.Active)
                    {
                        _logger.Error("Send command reply failed as remotingClient is not connected, replyAddress: " + context.ReplyAddress);
                        return;
                    }

                    var message = _jsonSerializer.Serialize(context.ReplyData);
                    var body = Encoding.UTF8.GetBytes(message);
                    var request = new Request()
                    {
                        Code = context.ReplyType,
                        Body = body
                    };

                    remotingClient.Channel.WriteAndFlushAsync(request);
                }
                catch (Exception ex)
                {
                    _logger.Error("Send command reply has exeption, replyAddress: " + context.ReplyAddress, ex);
                }
            }, new SendReplyContext(replyType, replyData, replyAddress));
        }

        public async Task SendReplyAsync(short replyType, object replyData, string replyAddress)
        {
            var context = new SendReplyContext(replyType, replyData, replyAddress);
            try
            {
                var remotingClient = GetRemotingClient(context.ReplyAddress);
                if (remotingClient == null) return;

                if (!remotingClient.Channel.Active)
                {
                    _logger.Error("Send command reply failed as remotingClient is not connected, replyAddress: " + context.ReplyAddress);
                    return;
                }

                var message = _jsonSerializer.Serialize(context.ReplyData);
                var body = Encoding.UTF8.GetBytes(message);
                var request = new Request()
                {
                    Code = context.ReplyType,
                    Body = body
                };

                await remotingClient.Channel.WriteAndFlushAsync(request);
            }
            catch (Exception ex)
            {
                _logger.Error("Send command reply has exeption, replyAddress: " + context.ReplyAddress, ex);
            }
        }

        public void Start()
        {
            _scheduleService.StartTask(_scanInactiveCommandRemotingClientTaskName, ScanInactiveRemotingClients, 5000, 5000);
        }

        public void Stop()
        {
            _scheduleService.StopTask(_scanInactiveCommandRemotingClientTaskName);
            foreach (var remotingClient in _remotingClientDict.Values)
            {
                remotingClient.Shutdown();
            }
        }

        private NettyClient CreateReplyRemotingClient(string replyAddress, IPEndPoint replyEndpoint)
        {
            lock (lockObject)
            {
                return _remotingClientDict.GetOrAdd(replyAddress, key =>
                {
                    return new NettyClient(replyEndpoint, null).Start();
                });
            }
        }

        private NettyClient GetRemotingClient(string replyAddress)
        {
            var replyEndpoint = TryParseReplyAddress(replyAddress);
            if (replyEndpoint == null) return null;

            NettyClient remotingClient;
            if (_remotingClientDict.TryGetValue(replyAddress, out remotingClient))
            {
                return remotingClient;
            }

            _ioHelper.TryIOAction("CreateReplyRemotingClient", () => "replyAddress:" + replyAddress, () => CreateReplyRemotingClient(replyAddress, replyEndpoint), 3);

            if (_remotingClientDict.TryGetValue(replyAddress, out remotingClient))
            {
                return remotingClient;
            }

            return null;
        }

        private void ScanInactiveRemotingClients()
        {
            var inactiveList = new List<KeyValuePair<string, NettyClient>>();
            foreach (var pair in _remotingClientDict)
            {
                if (pair.Value.Channel == null || !pair.Value.Channel.Active)
                {
                    inactiveList.Add(pair);
                }
            }
            foreach (var pair in inactiveList)
            {
                NettyClient removed;
                if (_remotingClientDict.TryRemove(pair.Key, out removed))
                {
                    _logger.InfoFormat("Removed disconnected command remoting client, remotingAddress: {0}", pair.Key);
                }
            }
        }

        private IPEndPoint TryParseReplyAddress(string replyAddress)
        {
            try
            {
                var items = replyAddress.Split(':');
                Ensure.Equals(items.Length, 2);
                var hostNameType = Uri.CheckHostName(items[0]);

                IPEndPoint ipEndPoint;
                switch (hostNameType)
                {
                    case UriHostNameType.Dns:
                        ipEndPoint = SocketUtils.GetIPEndPointFromHostName(items[0], int.Parse(items[1]), AddressFamily.InterNetwork, false);
                        break;

                    case UriHostNameType.IPv4:
                    case UriHostNameType.IPv6:
                        ipEndPoint = new IPEndPoint(IPAddress.Parse(items[0]), int.Parse(items[1]));
                        break;

                    case UriHostNameType.Unknown:
                    default:
                        throw new Exception($"Host name type[{hostNameType}] can not resolve.");
                }

                return new IPEndPoint(IPAddress.Parse(items[0]), int.Parse(items[1]));
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Invalid reply address : {0}", replyAddress), ex);
                return null;
            }
        }

        private class SendReplyContext
        {
            public SendReplyContext(short replyType, object replyData, string replyAddress)
            {
                ReplyType = replyType;
                ReplyData = replyData;
                ReplyAddress = replyAddress;
            }

            public string ReplyAddress { get; private set; }
            public object ReplyData { get; private set; }
            public short ReplyType { get; private set; }
        }
    }
}