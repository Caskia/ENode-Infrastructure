using ENode.Configurations;

namespace ENode.EventStore.MongoDb.Configurations
{
    public class EventStreamCollectionConfiguration : ShardableCollectionConfiguration, IEventStreamCollectionConfiguration
    {
    }
}