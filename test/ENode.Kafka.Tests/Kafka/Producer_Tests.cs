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
    public class Producer_Tests : KafkaTestBase
    {
        public ProducerSetting GetProducerSetting()
        {
            var strBrokerAddresses = Root["Kafka:BrokerAddresses"];
            var ipEndPoints = new List<IPEndPoint>();
            var addressList = strBrokerAddresses.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            foreach (var address in addressList)
            {
                var array = address.Split(new string[] { ":" }, StringSplitOptions.RemoveEmptyEntries);
                var endpoint = SocketUtils.GetIPEndPointFromHostName(array[0], int.Parse(array[1]));
                ipEndPoints.Add(endpoint);
            }

            return new ProducerSetting()
            {
                BrokerEndPoints = ipEndPoints
            };
        }

        [Fact(DisplayName = "Should_Produce")]
        public async Task Should_Produce()
        {
            //Arrange
            var producer = new Producer(GetProducerSetting());
            (string topic, string routingKey, string content) message = ("test", "1234", "testtesttesttest");

            //Act
            var result = await producer.ProduceAsync(message.topic, message.routingKey, message.content);

            //Assert
            result.Key.ShouldBe(message.routingKey);
        }

        [Fact(DisplayName = "Should_Produce_Concurrent")]
        public async Task Should_Produce_Concurrent()
        {
            //Arrange
            var producer = new Producer(GetProducerSetting());
            (string topic, string routingKey, string content) message = ("test", "1234", Encoding.UTF8.GetString(new byte[2048]));

            //Act
            var tasks = new List<Task>();

            for (int i = 0; i < 100000; i++)
            {
                var task = producer.ProduceAsync(message.topic, message.routingKey, message.content);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
        }
    }
}