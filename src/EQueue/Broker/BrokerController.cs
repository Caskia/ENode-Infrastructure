﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Extensions;
using ECommon.Logging;
using ECommon.Remoting;
using ECommon.Scheduling;
using ECommon.Serializing;
using ECommon.Socketing;
using ECommon.Utilities;
using EQueue.Broker.Client;
using EQueue.Broker.LongPolling;
using EQueue.Broker.RequestHandlers;
using EQueue.Broker.RequestHandlers.Admin;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.NameServers;
using EQueue.Protocols.NameServers.Requests;

namespace EQueue.Broker
{
    public class BrokerController
    {
        private static BrokerController _instance;
        private readonly SocketRemotingServer _adminSocketRemotingServer;
        private readonly IBinarySerializer _binarySerializer;
        private readonly IChunkStatisticService _chunkReadStatisticService;
        private readonly IConsumeOffsetStore _consumeOffsetStore;
        private readonly ConsumerManager _consumerManager;
        private readonly SocketRemotingServer _consumerSocketRemotingServer;
        private readonly GetConsumerListService _getConsumerListService;
        private readonly GetTopicConsumeInfoListService _getTopicConsumeInfoListService;
        private readonly ILogger _logger;
        private readonly IMessageStore _messageStore;
        private readonly IList<SocketRemotingClient> _nameServerRemotingClientList;
        private readonly ProducerManager _producerManager;
        private readonly SocketRemotingServer _producerSocketRemotingServer;
        private readonly IQueueStore _queueStore;
        private readonly IScheduleService _scheduleService;
        private readonly SuspendedPullRequestManager _suspendedPullRequestManager;
        private readonly ITpsStatisticService _tpsStatisticService;
        private int _isCleaning = 0;
        private int _isShuttingdown = 0;
        private string[] _latestMessageIds;
        private long _messageIdSequece;

        private BrokerController(BrokerSetting setting)
        {
            Setting = setting ?? new BrokerSetting();

            Setting.BrokerInfo.Valid();
            if (Setting.NameServerList == null || Setting.NameServerList.Count() == 0)
            {
                throw new ArgumentException("NameServerList is empty.");
            }

            _latestMessageIds = new string[Setting.LatestMessageShowCount];
            _producerManager = ObjectContainer.Resolve<ProducerManager>();
            _consumerManager = ObjectContainer.Resolve<ConsumerManager>();
            _messageStore = ObjectContainer.Resolve<IMessageStore>();
            _consumeOffsetStore = ObjectContainer.Resolve<IConsumeOffsetStore>();
            _queueStore = ObjectContainer.Resolve<IQueueStore>();
            _getTopicConsumeInfoListService = ObjectContainer.Resolve<GetTopicConsumeInfoListService>();
            _getConsumerListService = ObjectContainer.Resolve<GetConsumerListService>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _suspendedPullRequestManager = ObjectContainer.Resolve<SuspendedPullRequestManager>();
            _chunkReadStatisticService = ObjectContainer.Resolve<IChunkStatisticService>();
            _tpsStatisticService = ObjectContainer.Resolve<ITpsStatisticService>();

            _producerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ProducerRemotingServer", Setting.BrokerInfo.ProducerAddress.ToEndPoint(), Setting.SocketSetting);
            _consumerSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.ConsumerRemotingServer", Setting.BrokerInfo.ConsumerAddress.ToEndPoint(), Setting.SocketSetting);
            _adminSocketRemotingServer = new SocketRemotingServer("EQueue.Broker.AdminRemotingServer", Setting.BrokerInfo.AdminAddress.ToEndPoint(), Setting.SocketSetting);

            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _producerSocketRemotingServer.RegisterConnectionEventListener(new ProducerConnectionEventListener(this));
            _consumerSocketRemotingServer.RegisterConnectionEventListener(new ConsumerConnectionEventListener(this));
            RegisterRequestHandlers();
            _nameServerRemotingClientList = Setting.NameServerList.ToRemotingClientList("EQueueBroker." + Setting.BrokerInfo.BrokerName, Setting.SocketSetting).ToList();
        }

        public static BrokerController Instance
        {
            get { return _instance; }
        }

        public ConsumerManager ConsumerManager
        {
            get { return _consumerManager; }
        }

        public bool IsCleaning
        {
            get { return _isCleaning == 1; }
        }

        public ProducerManager ProducerManager
        {
            get { return _producerManager; }
        }

        public BrokerSetting Setting { get; private set; }

        public static BrokerController Create(BrokerSetting setting = null)
        {
            _instance = new BrokerController(setting);
            return _instance;
        }

        public void AddLatestMessage(string messageId, DateTime createTime, DateTime storedTime)
        {
            var sequence = Interlocked.Increment(ref _messageIdSequece);
            var index = sequence % _latestMessageIds.Length;
            _latestMessageIds[index] = string.Format("{0}_{1}_{2}", messageId, createTime.Ticks, storedTime.Ticks);
        }

        public BrokerController Clean()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("Broker clean starting...");

            if (Interlocked.CompareExchange(ref _isCleaning, 1, 0) == 0)
            {
                try
                {
                    //首先关闭所有组件
                    _queueStore.Shutdown();
                    _consumeOffsetStore.Shutdown();
                    _messageStore.Shutdown();
                    _suspendedPullRequestManager.Clean();

                    //再删除Broker的整个存储目录以及目录下的所有文件
                    if (Directory.Exists(Setting.FileStoreRootPath))
                    {
                        Directory.Delete(Setting.FileStoreRootPath, true);
                    }

                    //再重新加载和启动所有组件
                    _messageStore.Load();
                    _queueStore.Load();

                    _consumeOffsetStore.Start();
                    _messageStore.Start();
                    _queueStore.Start();

                    Interlocked.Exchange(ref _isCleaning, 0);
                    _logger.InfoFormat("Broker clean success, timeSpent:{0}ms, producer:[{1}], consumer:[{2}], admin:[{3}]", watch.ElapsedMilliseconds, Setting.BrokerInfo.ProducerAddress, Setting.BrokerInfo.ConsumerAddress, Setting.BrokerInfo.AdminAddress);
                }
                catch (Exception ex)
                {
                    _logger.ErrorFormat("Broker clean failed.", ex);
                    throw;
                }
            }

            return this;
        }

        public BrokerStatisticInfo GetBrokerStatisticInfo()
        {
            var statisticInfo = new BrokerStatisticInfo();
            statisticInfo.BrokerInfo = Setting.BrokerInfo;
            statisticInfo.TopicCount = _queueStore.GetAllTopics().Count();
            statisticInfo.QueueCount = _queueStore.GetAllQueueCount();
            statisticInfo.TotalUnConsumedMessageCount = _queueStore.GetTotalUnConusmedMessageCount();
            statisticInfo.ConsumerGroupCount = _consumeOffsetStore.GetConsumerGroupCount();
            statisticInfo.ProducerCount = _producerManager.GetProducerCount();
            statisticInfo.ConsumerCount = _consumerManager.GetAllConsumerCount();
            statisticInfo.MessageChunkCount = _messageStore.ChunkCount;
            statisticInfo.MessageMinChunkNum = _messageStore.MinChunkNum;
            statisticInfo.MessageMaxChunkNum = _messageStore.MaxChunkNum;
            statisticInfo.TotalSendThroughput = _tpsStatisticService.GetTotalSendThroughput();
            statisticInfo.TotalConsumeThroughput = _tpsStatisticService.GetTotalConsumeThroughput();
            return statisticInfo;
        }

        public string GetLatestSendMessageIds()
        {
            return string.Join(",", _latestMessageIds.ToList());
        }

        public BrokerController Shutdown()
        {
            if (Interlocked.CompareExchange(ref _isShuttingdown, 1, 0) == 0)
            {
                var watch = Stopwatch.StartNew();
                _logger.InfoFormat("Broker starting to shutdown, producer:[{0}], consumer:[{1}], admin:[{2}]", Setting.BrokerInfo.ProducerAddress, Setting.BrokerInfo.ConsumerAddress, Setting.BrokerInfo.AdminAddress);
                _scheduleService.StopTask("RegisterBrokerToAllNameServers");
                UnregisterBrokerToAllNameServers().ConfigureAwait(false).GetAwaiter().GetResult();
                StopAllNameServerClients();
                _producerSocketRemotingServer.Shutdown();
                _consumerSocketRemotingServer.Shutdown();
                _adminSocketRemotingServer.Shutdown();
                _producerManager.Shutdown();
                _consumerManager.Shutdown();
                _suspendedPullRequestManager.Shutdown();
                _messageStore.Shutdown();
                _consumeOffsetStore.Shutdown();
                _queueStore.Shutdown();
                _chunkReadStatisticService.Shutdown();
                _tpsStatisticService.Shutdown();
                _logger.InfoFormat("Broker shutdown success, timeSpent:{0}ms", watch.ElapsedMilliseconds);
            }
            return this;
        }

        public BrokerController Start()
        {
            var watch = Stopwatch.StartNew();
            _logger.InfoFormat("Broker starting...");

            _messageStore.Load();
            _queueStore.Load();

            if (_messageStore.ChunkCount == 0 || _queueStore.GetAllQueueCount() == 0)
            {
                _logger.InfoFormat("The message store or queue store is empty, try to clear all the broker store files.");

                _messageStore.Shutdown();
                _queueStore.Shutdown();

                if (Directory.Exists(Setting.FileStoreRootPath))
                {
                    Directory.Delete(Setting.FileStoreRootPath, true);
                }

                _logger.InfoFormat("All the broker store files clear success.");

                _messageStore.Load();
                _queueStore.Load();
            }

            _consumeOffsetStore.Start();
            _messageStore.Start();
            _queueStore.Start();
            _producerManager.Start();
            _consumerManager.Start();
            _suspendedPullRequestManager.Start();
            _consumerSocketRemotingServer.Start();
            _producerSocketRemotingServer.Start();
            _adminSocketRemotingServer.Start();
            _chunkReadStatisticService.Start();
            _tpsStatisticService.Start();

            RemoveNotExistQueueConsumeOffsets();
            StartAllNameServerClients();
            RegisterBrokerToAllNameServers(true).ConfigureAwait(false).GetAwaiter().GetResult();
            _scheduleService.StartTask("RegisterBrokerToAllNameServers", async () => await RegisterBrokerToAllNameServers(), 1000, Setting.RegisterBrokerToNameServerInterval);

            Interlocked.Exchange(ref _isShuttingdown, 0);
            _logger.InfoFormat("Broker started, timeSpent:{0}ms, producer:[{1}], consumer:[{2}], admin:[{3}]", watch.ElapsedMilliseconds, Setting.BrokerInfo.ProducerAddress, Setting.BrokerInfo.ConsumerAddress, Setting.BrokerInfo.AdminAddress);
            return this;
        }

        private async Task RegisterBrokerToAllNameServers(bool isFirstTime = false)
        {
            var totalSendThroughput = _tpsStatisticService.GetTotalSendThroughput();
            var totalConsumeThroughput = _tpsStatisticService.GetTotalConsumeThroughput();
            var topicQueueInfoList = _queueStore.GetTopicQueueInfoList();
            var topicConsumeInfoList = _getTopicConsumeInfoListService.GetAllTopicConsumeInfoList().ToList();
            var producerList = _producerManager.GetAllProducers().ToList();
            var consumerList = _getConsumerListService.GetAllConsumerList().ToList();
            var request = new BrokerRegistrationRequest
            {
                BrokerInfo = Setting.BrokerInfo,
                TotalSendThroughput = totalSendThroughput,
                TotalConsumeThroughput = totalConsumeThroughput,
                TotalUnConsumedMessageCount = _queueStore.GetTotalUnConusmedMessageCount(),
                TopicQueueInfoList = topicQueueInfoList,
                TopicConsumeInfoList = topicConsumeInfoList,
                ProducerList = producerList,
                ConsumerList = consumerList
            };
            foreach (var remotingClient in _nameServerRemotingClientList)
            {
                await RegisterBrokerToNameServer(request, remotingClient, isFirstTime);
            }
        }

        private async Task RegisterBrokerToNameServer(BrokerRegistrationRequest request, SocketRemotingClient remotingClient, bool isFirstTime = false)
        {
            var nameServerAddress = remotingClient.ServerEndPoint.ToAddress();
            try
            {
                var data = _binarySerializer.Serialize(request);
                var remotingRequest = new RemotingRequest((int)NameServerRequestCode.RegisterBroker, data);
                var remotingResponse = await remotingClient.InvokeAsync(remotingRequest, 10 * 1000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    _logger.ErrorFormat("Register broker to name server failed, brokerInfo: {0}, nameServerAddress: {1}, remoting response code: {2}, errorMessage: {3}", request.BrokerInfo, nameServerAddress, remotingResponse.ResponseCode, Encoding.UTF8.GetString(remotingResponse.ResponseBody));
                }
                else if (isFirstTime)
                {
                    _logger.InfoFormat("Register broker to name server success, brokerInfo: {0}, nameServerAddress: {1}", request.BrokerInfo, nameServerAddress);
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Register broker to name server has exception, brokerInfo: {0}, nameServerAddress: {1}", request.BrokerInfo, nameServerAddress), ex);
            }
        }

        private void RegisterRequestHandlers()
        {
            _producerSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.ProducerHeartbeat, new ProducerHeartbeatRequestHandler(this));
            _producerSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.SendMessage, new SendMessageRequestHandler(this));
            _producerSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.BatchSendMessage, new BatchSendMessageRequestHandler(this));

            _consumerSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.ConsumerHeartbeat, new ConsumerHeartbeatRequestHandler(this));
            _consumerSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.PullMessage, new PullMessageRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetConsumerIdsForTopic, new GetConsumerIdsForTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.UpdateQueueConsumeOffsetRequest, new UpdateQueueConsumeOffsetRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetTopicConsumeInfo, new GetTopicConsumeInfoRequestHandler());

            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetBrokerStatisticInfo, new GetBrokerStatisticInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.CreateTopic, new CreateTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.DeleteTopic, new DeleteTopicRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetTopicQueueInfo, new GetTopicQueueInfoRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetProducerList, new GetProducerListRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetConsumerList, new GetConsumerListRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.AddQueue, new AddQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.DeleteQueue, new DeleteQueueRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.SetQueueProducerVisible, new SetQueueProducerVisibleRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.SetQueueConsumerVisible, new SetQueueConsumerVisibleRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetMessageDetail, new GetMessageDetailRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetMessageDetailByQueueOffset, new GetMessageDetailByQueueOffsetRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.SetQueueNextConsumeOffset, new SetQueueNextConsumeOffsetRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.DeleteConsumerGroup, new DeleteConsumerGroupRequestHandler());
            _adminSocketRemotingServer.RegisterRequestHandler((int)BrokerRequestCode.GetLastestMessages, new GetBrokerLatestSendMessagesRequestHandler());
        }

        private void RemoveNotExistQueueConsumeOffsets()
        {
            var consumeKeys = _consumeOffsetStore.GetConsumeKeys();
            foreach (var consumeKey in consumeKeys)
            {
                if (!_queueStore.IsQueueExist(consumeKey))
                {
                    _consumeOffsetStore.DeleteConsumeOffset(consumeKey);
                }
            }
        }

        private void StartAllNameServerClients()
        {
            foreach (var nameServerRemotingClient in _nameServerRemotingClientList)
            {
                nameServerRemotingClient.Start();
            }
        }

        private void StopAllNameServerClients()
        {
            foreach (var nameServerRemotingClient in _nameServerRemotingClientList)
            {
                nameServerRemotingClient.Shutdown();
            }
        }

        private async Task UnregisterBrokerToAllNameServers()
        {
            var request = new BrokerUnRegistrationRequest
            {
                BrokerInfo = Setting.BrokerInfo
            };
            foreach (var remotingClient in _nameServerRemotingClientList)
            {
                await UnregisterBrokerToNameServer(request, remotingClient);
            }
        }

        private async Task UnregisterBrokerToNameServer(BrokerUnRegistrationRequest request, SocketRemotingClient remotingClient)
        {
            if (!remotingClient.IsConnected)
            {
                return;
            }
            var nameServerAddress = remotingClient.ServerEndPoint.ToAddress();
            try
            {
                var data = _binarySerializer.Serialize(request);
                var remotingRequest = new RemotingRequest((int)NameServerRequestCode.UnregisterBroker, data);
                var remotingResponse = await remotingClient.InvokeAsync(remotingRequest, 10 * 1000);
                if (remotingResponse.ResponseCode != ResponseCode.Success)
                {
                    _logger.Error(string.Format("Unregister broker from name server failed, brokerInfo: {0}, nameServerAddress: {1}, remoting response code: {2}, errorMessage: {3}", request.BrokerInfo, nameServerAddress, remotingResponse.ResponseCode, Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
                }
            }
            catch (Exception ex)
            {
                _logger.Error(string.Format("Unregister broker from name server has exception, brokerInfo: {0}, nameServerAddress: {1}", request.BrokerInfo, nameServerAddress), ex);
            }
        }

        private class ConsumerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;

            public ConsumerConnectionEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnConnectionAccepted(ITcpConnection connection)
            {
            }

            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var connectionId = connection.RemotingEndPoint.ToAddress();
                if (_brokerController.Setting.RemoveConsumerWhenDisconnect)
                {
                    _brokerController._consumerManager.RemoveConsumer(connectionId);
                }
            }

            public void OnConnectionEstablished(ITcpConnection connection)
            {
            }

            public void OnConnectionFailed(EndPoint remotingEndPoint, SocketError socketError)
            {
            }
        }

        private class ProducerConnectionEventListener : IConnectionEventListener
        {
            private BrokerController _brokerController;

            public ProducerConnectionEventListener(BrokerController brokerController)
            {
                _brokerController = brokerController;
            }

            public void OnConnectionAccepted(ITcpConnection connection)
            {
            }

            public void OnConnectionClosed(ITcpConnection connection, SocketError socketError)
            {
                var connectionId = connection.RemotingEndPoint.ToAddress();
                _brokerController._producerManager.RemoveProducer(connectionId);
            }

            public void OnConnectionEstablished(ITcpConnection connection)
            {
            }

            public void OnConnectionFailed(EndPoint remotingEndPoint, SocketError socketError)
            {
            }
        }
    }
}