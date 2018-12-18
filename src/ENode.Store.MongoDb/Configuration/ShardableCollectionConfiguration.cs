namespace ENode.Store.MongoDb.Configuration
{
    public class ShardableCollectionConfiguration : IShardableCollectionConfiguration
    {
        public string EntityName { get; set; }

        public int ShardCount { get; set; }
    }
}