using Confluent.Kafka;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Utilities;
using ENode.Kafka.Consumers;
using ENode.Kafka.Producers;
using ENode.Kafka.Utils;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ENode.Kafka.Tests.Kafka
{
    public class Producer_Tests : ENodeKafkaTestBase
    {
        public IList<IPEndPoint> GetBrokerEndPoints()
        {
            var strBrokerAddresses = ENodeKafkaFixture.Root["Kafka:BrokerAddresses"];
            var ipEndPoints = new List<IPEndPoint>();
            var addressList = strBrokerAddresses.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var address in addressList)
            {
                var array = address.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                var endpoint = SocketUtils.GetIPEndPointFromHostName(array[0], int.Parse(array[1]));
                ipEndPoints.Add(endpoint);
            }

            return ipEndPoints;
        }

        public ConsumerSetting GetConsumerSetting()
        {
            return new ConsumerSetting()
            {
                BrokerEndPoints = GetBrokerEndPoints()
            };
        }

        public ProducerSetting GetProducerSetting()
        {
            return new ProducerSetting()
            {
                BrokerEndPoints = GetBrokerEndPoints()
            };
        }

        [Fact]
        public void Should_Consume()
        {
            var loggfactory = ObjectContainer.Resolve<ILoggerFactory>();
            var logger = loggfactory.Create(nameof(Producer_Tests));

            var consumer = new Consumer(GetConsumerSetting());
            consumer.Subscribe(new List<string>()
            {
                ObjectId.GenerateNewStringId(),
                ObjectId.GenerateNewStringId(),
                ObjectId.GenerateNewStringId(),
                ObjectId.GenerateNewStringId(),
                "CommandTopic",
                //"EventTopic",
                //"ApplicationMessageTopic",
                //"DomainExceptionTopic"
            });
            consumer.OnLog += (_, info) =>
            {
                logger.InfoFormat(info.Message);
            };
            consumer.OnError = (_, error) =>
            {
            };

            consumer
                .SetMessageHandler(new TestMessageHandler())
                .Start();
        }

        [Fact(DisplayName = "Should_Produce")]
        public async Task Should_Produce()
        {
            //Arrange
            var producer = new Producer(GetProducerSetting());
            (string topic, string routingKey, string content) = ("test", "1234", "testtesttesttest");

            //Act
            var result = await producer.ProduceAsync(topic, routingKey, content);

            //Assert
            result.Key.ShouldBe(routingKey);
        }

        [Fact(DisplayName = "Should_Produce_Concurrent")]
        public async Task Should_Produce_Concurrent()
        {
            //Arrange
            var producer = new Producer(GetProducerSetting());
            (string topic, string routingKey, string content) = ("test", "1234", Encoding.UTF8.GetString(new byte[2048]));

            //Act
            var tasks = new List<Task>();

            for (int i = 0; i < 100000; i++)
            {
                var task = producer.ProduceAsync(topic, routingKey, content);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }

    public class TestMessageHandler : IMessageHandler<Ignore, string>
    {
        public Task HandleAsync(ConsumeResult<Ignore, string> message, IMessageContext<Ignore, string> context)
        {
            return Task.CompletedTask;
        }
    }
}