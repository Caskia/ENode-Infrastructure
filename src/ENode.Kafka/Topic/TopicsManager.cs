using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.Kafka
{
    public class TopicsManager
    {
        private readonly AdminClientConfig _adminClientConfig;

        public TopicsManager(string bootstrapServers)
        {
            _adminClientConfig = new AdminClientConfig(new ClientConfig()
            {
                BootstrapServers = bootstrapServers
            });
        }

        public async Task CheckAndCreateTopicsAsync(IEnumerable<string> topics)
        {
            using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();

            var metaData = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
            var existedTopics = metaData.Topics.Select(t => t.Topic);
            var needToCreateTopics = topics.Where(t => !existedTopics.Contains(t));
            if (!needToCreateTopics.Any())
            {
                return;
            }

            await adminClient.CreateTopicsAsync(needToCreateTopics.Select(t => new TopicSpecification()
            {
                Name = t
            }));

            while (true)
            {
                var data = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
                var allTopics = data.Topics.Select(t => t.Topic);

                if (needToCreateTopics.All(a => allTopics.Contains(a)))
                {
                    break;
                }

                await Task.Delay(5000);
            }
        }
    }
}