using Xunit;

namespace ENode.Kafka.Tests
{
    [CollectionDefinition(nameof(ENodeKafkaCollection))]
    public class ENodeKafkaCollection : ICollectionFixture<ENodeKafkaFixture>
    {
    }
}