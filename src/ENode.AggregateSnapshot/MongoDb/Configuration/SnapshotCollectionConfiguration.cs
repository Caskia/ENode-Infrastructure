using ENode.Store.MongoDb.Configuration;

namespace ENode.AggregateSnapshot.Configuration
{
    public class SnapshotCollectionConfiguration : ShardableCollectionConfiguration, ISnapshotCollectionConfiguration
    {
    }
}