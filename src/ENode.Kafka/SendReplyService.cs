using ECommon.Components;
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
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly string _name;
        private readonly ConcurrentDictionary<string, Lazy<Task<NettyClient>>> _remotingClientDict;
        private readonly string _scanInactiveCommandRemotingClientTaskName;
        private readonly IScheduleService _scheduleService;

        public SendReplyService(string name)
        {
            _name = name;
            _remotingClientDict = new ConcurrentDictionary<string, Lazy<Task<NettyClient>>>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _scanInactiveCommandRemotingClientTaskName = "ScanInactiveCommandRemotingClient_" + DateTime.Now.Ticks + new Random().Next(10000);
        }

        public void SendReply(short replyType, object replyData, string replyAddress)
        {
            Task.Factory.StartNew(async obj =>
            {
                var context = obj as SendReplyContext;
                try
                {
                    var remotingClient = await GetRemotingClientAsync(context.ReplyAddress);
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
            }, new SendReplyContext(replyType, replyData, replyAddress));
        }

        public async Task SendReplyAsync(short replyType, object replyData, string replyAddress)
        {
            var context = new SendReplyContext(replyType, replyData, replyAddress);
            try
            {
                var remotingClient = await GetRemotingClientAsync(context.ReplyAddress);

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
            _scheduleService.StartTask(_scanInactiveCommandRemotingClientTaskName, async () => await ScanInactiveRemotingClientsAsync(), 5000, 5000);
        }

        public void Stop()
        {
            Task.Run(async () =>
            {
                _scheduleService.StopTask(_scanInactiveCommandRemotingClientTaskName);
                foreach (var remotingClient in _remotingClientDict.Values)
                {
                    await (await remotingClient.Value).ShutdownAsync();
                }
            });
        }

        private async Task<NettyClient> GetOrCreateReplyRemotingClientAsync(string replyAddress, IPEndPoint replyEndpoint)
        {
            return await _remotingClientDict.GetOrAdd(replyAddress, new Lazy<Task<NettyClient>>(async () =>
            {
                var client = new NettyClient(replyEndpoint, null);

                await client.StartAsync();

                return client;
            })).Value;
        }

        private Task<NettyClient> GetRemotingClientAsync(string replyAddress)
        {
            var replyEndpoint = TryParseReplyAddress(replyAddress);
            if (replyEndpoint == null) return null;

            return GetOrCreateReplyRemotingClientAsync(replyAddress, replyEndpoint);
        }

        private async Task ScanInactiveRemotingClientsAsync()
        {
            var inactiveList = new List<string>();
            foreach (var pair in _remotingClientDict)
            {
                var client = (await pair.Value.Value);
                if (client.Channel == null || !client.Channel.Active)
                {
                    inactiveList.Add(pair.Key);
                }
            }
            foreach (var key in inactiveList)
            {
                if (_remotingClientDict.TryRemove(key, out Lazy<Task<NettyClient>> removed))
                {
                    await (await removed.Value).ShutdownAsync();
                    _logger.InfoFormat("Removed disconnected command remoting client, remotingAddress: {0}", key);
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