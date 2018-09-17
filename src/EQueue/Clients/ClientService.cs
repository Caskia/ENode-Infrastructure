﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
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
using EQueue.Clients.Consumers;
using EQueue.Clients.Producers;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.NameServers;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.Clients
{
    public class ClientService
    {
        #region Private Variables

        private static long _instanceNumber;
        private readonly AsyncLock _asyncLock = new AsyncLock();
        private readonly IBinarySerializer _binarySerializer;
        private readonly ConcurrentDictionary<string /*brokerName*/, BrokerConnection> _brokerConnectionDict;
        private readonly string _clientId;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;
        private readonly IList<SocketRemotingClient> _nameServerRemotingClientList;
        private readonly IScheduleService _scheduleService;
        private readonly ClientSetting _setting;
        private readonly ConcurrentDictionary<string /*topic*/, IList<MessageQueue>> _topicMessageQueueDict;
        private Consumer _consumer;
        private long _nameServerIndex;
        private Producer _producer;

        #endregion Private Variables

        public ClientService(ClientSetting setting, Producer producer, Consumer consumer)
        {
            Ensure.NotNull(setting, "setting");
            if (producer == null && consumer == null)
            {
                throw new ArgumentException("producer or consumer must set at least one of them.");
            }
            else if (producer != null && consumer != null)
            {
                throw new ArgumentException("producer or consumer cannot set both of them.");
            }

            Interlocked.Increment(ref _instanceNumber);

            _producer = producer;
            _consumer = consumer;
            _setting = setting;
            _clientId = BuildClientId(setting.ClientName);
            _brokerConnectionDict = new ConcurrentDictionary<string, BrokerConnection>();
            _topicMessageQueueDict = new ConcurrentDictionary<string, IList<MessageQueue>>();
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _scheduleService = ObjectContainer.Resolve<IScheduleService>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _nameServerRemotingClientList = _setting.NameServerList.ToRemotingClientList(_setting.ClientName, _setting.SocketSetting).ToList();
        }

        #region Public Methods

        public List<BrokerConnection> GetAllBrokerConnections()
        {
            return _brokerConnectionDict.Values.ToList();
        }

        public IList<MessageQueue> GetAvailableMessageQueues(string topic)
        {
            if (_topicMessageQueueDict.TryGetValue(topic, out IList<MessageQueue> messageQueueList))
            {
                return messageQueueList;
            }
            return null;
        }

        public BrokerConnection GetBrokerConnection(string brokerName)
        {
            BrokerConnection brokerConnection;
            if (_brokerConnectionDict.TryGetValue(brokerName, out brokerConnection))
            {
                return brokerConnection;
            }
            return null;
        }

        public string GetClientId()
        {
            return _clientId;
        }

        public BrokerConnection GetFirstBrokerConnection()
        {
            var availableList = _brokerConnectionDict.Values.Where(x => x.RemotingClient.IsConnected).ToList();
            if (availableList.Count == 0)
            {
                throw new Exception("No available broker.");
            }
            return availableList.First();
        }

        public async Task<IList<MessageQueue>> GetTopicMessageQueuesAsync(string topic)
        {
            if (_topicMessageQueueDict.TryGetValue(topic, out IList<MessageQueue> messageQueueList))
            {
                return messageQueueList;
            }

            using (await _asyncLock.LockAsync())
            {
                if (_topicMessageQueueDict.TryGetValue(topic, out messageQueueList))
                {
                    return messageQueueList;
                }
                try
                {
                    var topicRouteInfoList = await GetTopicRouteInfoListAsync(topic);
                    messageQueueList = new List<MessageQueue>();

                    foreach (var topicRouteInfo in topicRouteInfoList)
                    {
                        foreach (var queueId in topicRouteInfo.QueueInfo)
                        {
                            var messageQueue = new MessageQueue(topicRouteInfo.BrokerInfo.BrokerName, topic, queueId);
                            messageQueueList.Add(messageQueue);
                        }
                    }
                    SortMessageQueues(messageQueueList);
                    if (messageQueueList.IsEmpty())
                    {
                        _logger.WarnFormat("Topic route queue is empty, topic: {0}", topic);
                    }
                    _topicMessageQueueDict[topic] = messageQueueList;
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("GetTopicRouteInfoListAsync has exception, topic: {0}", topic), ex);
                }
                return messageQueueList;
            }
        }

        public ClientService RegisterSubscriptionTopic(string topic)
        {
            _topicMessageQueueDict.TryAdd(topic, new List<MessageQueue>());
            return this;
        }

        public virtual ClientService Start()
        {
            StartAllNameServerClients();
            RefreshClusterBrokers().ConfigureAwait(false).GetAwaiter().GetResult();
            _scheduleService.StartTask("SendHeartbeatToAllBrokers", SendHeartbeatToAllBrokers, 1000, _setting.SendHeartbeatInterval);
            _scheduleService.StartTask("RefreshBrokerAndTopicRouteInfo", async () =>
            {
                await RefreshClusterBrokers();
                await RefreshTopicRouteInfo();
            }, 1000, _setting.RefreshBrokerAndTopicRouteInfoInterval);
            _logger.InfoFormat("{0} startted.", GetType().Name);
            return this;
        }

        public virtual ClientService Stop()
        {
            _scheduleService.StopTask("SendHeartbeatToAllBrokers");
            _scheduleService.StopTask("RefreshBrokerAndTopicRouteInfo");
            StopAllNameServerClients();
            StopAllBrokerServices();
            _logger.InfoFormat("{0} stopped.", GetType().Name);
            return this;
        }

        #endregion Public Methods

        #region Private Methods

        private static string BuildClientId(string clientName)
        {
            var ip = SocketUtils.GetLocalIPV4().ToString();
            var processId = Process.GetCurrentProcess().Id;
            if (string.IsNullOrWhiteSpace(clientName))
            {
                clientName = "default";
            }
            return string.Format("{0}@{1}@{2}@{3}", ip, clientName, processId, _instanceNumber);
        }

        private BrokerConnection BuildAndStartBrokerConnection(BrokerInfo brokerInfo)
        {
            IPEndPoint brokerEndpoint;
            if (_producer != null)
            {
                brokerEndpoint = brokerInfo.ProducerAddress.ToEndPoint();
            }
            else if (_consumer != null)
            {
                brokerEndpoint = brokerInfo.ConsumerAddress.ToEndPoint();
            }
            else
            {
                throw new Exception("ClientService must set producer or consumer.");
            }
            var brokerAdminEndpoint = brokerInfo.AdminAddress.ToEndPoint();
            var remotingClient = new SocketRemotingClient(_setting.ClientName, brokerEndpoint, _setting.SocketSetting);
            var adminRemotingClient = new SocketRemotingClient(_setting.ClientName, brokerAdminEndpoint, _setting.SocketSetting);
            var brokerConnection = new BrokerConnection(brokerInfo, remotingClient, adminRemotingClient);

            if (_producer != null && _producer.ResponseHandler != null)
            {
                remotingClient.RegisterResponseHandler((int)BrokerRequestCode.SendMessage, _producer.ResponseHandler);
                remotingClient.RegisterResponseHandler((int)BrokerRequestCode.BatchSendMessage, _producer.ResponseHandler);
            }

            brokerConnection.Start();

            return brokerConnection;
        }

        private SocketRemotingClient GetAvailableNameServerRemotingClient()
        {
            var availableList = _nameServerRemotingClientList.Where(x => x.IsConnected).ToList();
            if (availableList.Count == 0)
            {
                throw new Exception("No available name server.");
            }
            return availableList[(int)(Interlocked.Increment(ref _nameServerIndex) % availableList.Count)];
        }

        private async Task<IList<BrokerInfo>> GetClusterBrokerListAsync()
        {
            var nameServerRemotingClient = GetAvailableNameServerRemotingClient();
            var request = new GetClusterBrokersRequest
            {
                ClusterName = _setting.ClusterName,
                OnlyFindMaster = _setting.OnlyFindMasterBroker
            };
            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetClusterBrokers, data);
            var remotingResponse = await nameServerRemotingClient.InvokeAsync(remotingRequest, 5 * 1000);
            if (remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception(string.Format("Get cluster brokers from name server failed, clusterName: {0}, nameServerAddress: {1}, remoting response code: {2}, errorMessage: {3}", request.ClusterName, nameServerRemotingClient.ServerEndPoint.ToAddress(), remotingResponse.ResponseCode, Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
            }
            return _binarySerializer.Deserialize<IList<BrokerInfo>>(remotingResponse.ResponseBody);
        }

        private async Task<IList<TopicRouteInfo>> GetTopicRouteInfoListAsync(string topic)
        {
            var nameServerRemotingClient = GetAvailableNameServerRemotingClient();
            var request = new GetTopicRouteInfoRequest
            {
                ClientRole = _producer != null ? ClientRole.Producer : ClientRole.Consumer,
                ClusterName = _setting.ClusterName,
                OnlyFindMaster = _setting.OnlyFindMasterBroker,
                Topic = topic
            };
            var data = _binarySerializer.Serialize(request);
            var remotingRequest = new RemotingRequest((int)NameServerRequestCode.GetTopicRouteInfo, data);
            var remotingResponse = await nameServerRemotingClient.InvokeAsync(remotingRequest, 5 * 1000);
            if (remotingResponse.ResponseCode != ResponseCode.Success)
            {
                throw new Exception(string.Format("Get topic route info from name server failed, topic: {0}, nameServerAddress: {1}, remoting response code: {2}, errorMessage: {3}", topic, nameServerRemotingClient.ServerEndPoint.ToAddress(), remotingResponse.ResponseCode, Encoding.UTF8.GetString(remotingResponse.ResponseBody)));
            }
            return _binarySerializer.Deserialize<IList<TopicRouteInfo>>(remotingResponse.ResponseBody);
        }

        private async Task RefreshClusterBrokers()
        {
            using (await _asyncLock.LockAsync())
            {
                var newBrokerInfoList = await GetClusterBrokerListAsync();
                var oldBrokerInfoList = _brokerConnectionDict.Select(x => x.Value.BrokerInfo).ToList();

                var newBrokerInfoJson = _jsonSerializer.Serialize(newBrokerInfoList);
                var oldBrokerInfoJson = _jsonSerializer.Serialize(oldBrokerInfoList);

                if (oldBrokerInfoJson != newBrokerInfoJson)
                {
                    var addedBrokerInfoList = newBrokerInfoList.Where(x => !_brokerConnectionDict.Any(y => y.Key == x.BrokerName)).ToList();
                    var removedBrokerServiceList = _brokerConnectionDict.Values.Where(x => !newBrokerInfoList.Any(y => y.BrokerName == x.BrokerInfo.BrokerName)).ToList();

                    foreach (var brokerInfo in addedBrokerInfoList)
                    {
                        var brokerConnection = BuildAndStartBrokerConnection(brokerInfo);
                        if (_brokerConnectionDict.TryAdd(brokerInfo.BrokerName, brokerConnection))
                        {
                            _logger.InfoFormat("Added broker: " + brokerInfo);
                        }
                    }
                    foreach (var brokerConnection in removedBrokerServiceList)
                    {
                        if (_brokerConnectionDict.TryRemove(brokerConnection.BrokerInfo.BrokerName, out BrokerConnection removed))
                        {
                            brokerConnection.Stop();
                            _logger.InfoFormat("Removed broker: " + brokerConnection.BrokerInfo);
                        }
                    }
                }
            }
        }

        private async Task RefreshTopicRouteInfo()
        {
            using (await _asyncLock.LockAsync())
            {
                foreach (var entry in _topicMessageQueueDict)
                {
                    var topic = entry.Key;
                    var oldMessageQueueList = entry.Value;
                    var topicRouteInfoList = await GetTopicRouteInfoListAsync(topic);
                    var newMessageQueueList = new List<MessageQueue>();

                    foreach (var topicRouteInfo in topicRouteInfoList)
                    {
                        foreach (var queueId in topicRouteInfo.QueueInfo)
                        {
                            var messageQueue = new MessageQueue(topicRouteInfo.BrokerInfo.BrokerName, topic, queueId);
                            newMessageQueueList.Add(messageQueue);
                        }
                    }
                    SortMessageQueues(newMessageQueueList);

                    var newMessageQueueJson = _jsonSerializer.Serialize(newMessageQueueList);
                    var oldMessageQueueJson = _jsonSerializer.Serialize(oldMessageQueueList);

                    if (oldMessageQueueJson != newMessageQueueJson)
                    {
                        _topicMessageQueueDict[topic] = newMessageQueueList;
                        _logger.InfoFormat("Topic routeInfo changed, topic: {0}, newRouteInfo: {1}, oldRouteInfo: {2}", topic, newMessageQueueJson, oldMessageQueueJson);
                    }
                }
            }
        }

        private void SendHeartbeatToAllBrokers()
        {
            if (_producer != null)
            {
                _producer.SendHeartbeat();
            }
            else if (_consumer != null)
            {
                _consumer.SendHeartbeat();
            }
        }

        private void SortMessageQueues(IList<MessageQueue> queueList)
        {
            ((List<MessageQueue>)queueList).Sort((x, y) =>
            {
                var brokerCompareResult = string.Compare(x.BrokerName, y.BrokerName);
                if (brokerCompareResult != 0)
                {
                    return brokerCompareResult;
                }
                else if (x.QueueId > y.QueueId)
                {
                    return 1;
                }
                else if (x.QueueId < y.QueueId)
                {
                    return -1;
                }
                return 0;
            });
        }

        private void StartAllNameServerClients()
        {
            foreach (var nameServerRemotingClient in _nameServerRemotingClientList)
            {
                nameServerRemotingClient.Start();
            }
        }

        private void StopAllBrokerServices()
        {
            foreach (var brokerService in _brokerConnectionDict.Values)
            {
                brokerService.Stop();
            }
        }

        private void StopAllNameServerClients()
        {
            foreach (var nameServerRemotingClient in _nameServerRemotingClientList)
            {
                nameServerRemotingClient.Shutdown();
            }
        }

        #endregion Private Methods
    }
}