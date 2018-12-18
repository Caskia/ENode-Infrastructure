namespace ENode.Configurations
{
    public interface IShardableCollectionConfiguration
    {
        string EntityName { get; set; }

        int ShardCount { get; set; }
    }
}