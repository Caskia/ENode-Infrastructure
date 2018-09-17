﻿using System;
using System.Text;
using ECommon.Components;
using ECommon.Remoting;
using ECommon.Serializing;
using EQueue.Protocols;
using EQueue.Protocols.Brokers;
using EQueue.Protocols.Brokers.Requests;
using EQueue.Protocols.NameServers.Requests;
using EQueue.Utils;

namespace EQueue.NameServer.RequestHandlers
{
    public class DeleteConsumerGroupForClusterRequestHandler : IRequestHandler
    {
        private NameServerController _nameServerController;
        private IBinarySerializer _binarySerializer;

        public DeleteConsumerGroupForClusterRequestHandler(NameServerController nameServerController)
        {
            _binarySerializer = ObjectContainer.Resolve<IBinarySerializer>();
            _nameServerController = nameServerController;
        }

        public RemotingResponse HandleRequest(IRequestHandlerContext context, RemotingRequest remotingRequest)
        {
            var request = _binarySerializer.Deserialize<DeleteConsumerGroupForClusterRequest>(remotingRequest.Body);
            var requestService = new BrokerRequestService(_nameServerController);

            requestService.ExecuteActionToAllClusterBrokers(request.ClusterName, async remotingClient =>
            {
                var requestData = _binarySerializer.Serialize(new DeleteConsumerGroupRequest(request.GroupName));
                var remotingResponse = await remotingClient.InvokeAsync(new RemotingRequest((int)BrokerRequestCode.DeleteConsumerGroup, requestData), 30000);
                context.SendRemotingResponse(remotingResponse);
            });

            return RemotingResponseFactory.CreateResponse(remotingRequest);
        }
    }
}
