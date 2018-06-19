using DotNetty.Transport.Channels;
using ECommon.Components;
using ENode.Kafka.Netty;
using Shouldly;
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

            var serverChannelHandlers = new List<IChannelHandler>();
            serverChannelHandlers.Add(ObjectContainer.Resolve<ServerHandler>());
            var server = new NettyServer(serverEndPoint, new NettyServerSetting() { ChannelHandlers = serverChannelHandlers });
            server.Start();

            var clientChannelHandlers = new List<IChannelHandler>();
            clientChannelHandlers.Add(ObjectContainer.Resolve<ClientHandler>());
            var client = new NettyClient(serverEndPoint, new NettyClientSetting() { ChannelHandlers = clientChannelHandlers });
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