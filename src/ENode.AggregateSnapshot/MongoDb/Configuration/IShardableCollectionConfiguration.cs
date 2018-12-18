namespace ENode.AggregateSnapshot.Configuration
{
    public interface IShardableCollectionConfiguration
    {
        string EntityName { get; set; }

        int ShardCount { get; set; }
    }
}