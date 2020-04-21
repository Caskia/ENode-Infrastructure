using DotNetty.Codecs;
using ECommon.Components;
using ENode.Kafka.Netty;
using ENode.Kafka.Netty.Codecs;
using Shouldly;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ENode.Kafka.Tests.Netty
{
    public class Netty_Tests : ENodeKafkaTestBase
    {
        [Fact(DisplayName = "Should_Communicates_Between_Server_And_Client")]
        public async Task Should_Communicates_Between_Server_And_Client()
        {
            //Arrange
            var serverEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9019);

            var serverSetting = new NettyServerSetting(
                channel =>
                {
                    var pipeline = channel.Pipeline;

                    pipeline.AddLast(typeof(LengthFieldPrepender).Name, new LengthFieldPrepender(2));
                    pipeline.AddLast(typeof(LengthFieldBasedFrameDecoder).Name, new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                    pipeline.AddLast(typeof(RequestEncoder).Name, new RequestEncoder());
                    pipeline.AddLast(typeof(RequestDecoder).Name, new RequestDecoder());
                    pipeline.AddLast(typeof(ServerHandler).Name, new ServerHandler(ObjectContainer.Resolve<ServerMessageBox>()));
                }
            );
            var server = new NettyServer(serverEndPoint, serverSetting);
            server.Start();

            var clientChannelHandlerTypes = new List<ChannelHandlerInstance>();
            clientChannelHandlerTypes.Add(new ChannelHandlerInstance() { Type = typeof(ClientHandler), Args = new List<object>() { ObjectContainer.Resolve<ClientMessageBox>() } });

            var clientSetting = new NettyClientSetting(
                channel =>
                {
                    var pipeline = channel.Pipeline;

                    pipeline.AddLast(typeof(LengthFieldPrepender).Name, new LengthFieldPrepender(2));
                    pipeline.AddLast(typeof(LengthFieldBasedFrameDecoder).Name, new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                    pipeline.AddLast(typeof(RequestEncoder).Name, new RequestEncoder());
                    pipeline.AddLast(typeof(RequestDecoder).Name, new RequestDecoder());
                    pipeline.AddLast(typeof(ClientHandler).Name, new ClientHandler(ObjectContainer.Resolve<ClientMessageBox>()));
                }
            );
            var client = new NettyClient(serverEndPoint, clientSetting);
            await client.StartAsync();

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