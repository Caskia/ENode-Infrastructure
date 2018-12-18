namespace ENode.AggregateSnapshot.Configuration
{
    public class SnapshotCollectionConfiguration : ISnapshotCollectionConfiguration
    {
        public string EntityName { get; set; }

        public int ShardCount { get; set; }
    }
}