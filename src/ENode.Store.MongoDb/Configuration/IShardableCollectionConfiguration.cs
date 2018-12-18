namespace ENode.Store.MongoDb.Configuration
{
    public interface IShardableCollectionConfiguration
    {
        string EntityName { get; set; }

        int ShardCount { get; set; }
    }
}