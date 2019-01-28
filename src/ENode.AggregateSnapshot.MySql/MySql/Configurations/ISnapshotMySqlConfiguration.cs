namespace ENode.AggregateSnapshot.Configurations
{
    public interface ISnapshotMySqlConfiguration
    {
        string ConnectionString { get; set; }

        int TableCount { get; set; }

        string TableName { get; set; }
    }
}