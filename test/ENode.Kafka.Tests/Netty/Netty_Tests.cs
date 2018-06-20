using DotNetty.Common.Internal.Logging;
using DotNetty.Transport.Channels;
using ECommon.Components;
using ENode.Kafka.Netty;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ENode.Kafka.Tests.Netty
{
    public class Netty_Tests : TestBase
    {
        [Fact(DisplayName = "Should_Communicates_Between_Server_And_Client")]
        public async Task Should_Communicates_Between_Server_And_Client()
        {
            //Arrange
            var serverEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9019);

            var serverChannelHandlerTypes = new List<ChannelHandlerInstance>();
            serverChannelHandlerTypes.Add(new ChannelHandlerInstance() { Type = typeof(ServerHandler), Args = new List<object>() { ObjectContainer.Resolve<ServerMessageBox>() } });
            var server = new NettyServer(serverEndPoint, new NettyServerSetting() { ChannelHandlerInstances = serverChannelHandlerTypes });
            server.Start();

            var clientChannelHandlerTypes = new List<ChannelHandlerInstance>();
            clientChannelHandlerTypes.Add(new ChannelHandlerInstance() { Type = typeof(ClientHandler), Args = new List<object>() { ObjectContainer.Resolve<ClientMessageBox>() } });
            var client = new NettyClient(serverEndPoint, new NettyClientSetting() { ChannelHandlerInstances = clientChannelHandlerTypes });
            client.Start();

            var request = new Request()
            {
                Code = 1,
                Body = Encoding.UTF8.GetBytes("test")
            };

            //Act
            await client.Channel.WriteAndFlushAsync(request);

            await Task.Delay(300);

            //Assert
            var serverMessageBox = ObjectContainer.Resolve<ServerMessageBox>();
            var messages = await serverMessageBox.GetAllAsync();
            messages.Count.ShouldBe(1);
            messages.Select(m => m as Request).FirstOrDefault(m => m.Code == request.Code).ShouldNotBeNull();
        }
    }
}