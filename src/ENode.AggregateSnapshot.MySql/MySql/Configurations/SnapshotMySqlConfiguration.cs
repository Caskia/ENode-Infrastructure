namespace ENode.AggregateSnapshot.Configurations
{
    public class SnapshotMySqlConfiguration : ISnapshotMySqlConfiguration
    {
        public string ConnectionString { get; set; }

        public int TableCount { get; set; }

        public string TableName { get; set; }
    }
}