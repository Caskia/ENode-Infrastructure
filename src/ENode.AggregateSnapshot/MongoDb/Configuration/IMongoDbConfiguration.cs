namespace ENode.AggregateSnapshot.Configuration
{
    public interface IMongoDbConfiguration
    {
        string ConnectionString { get; set; }

        string DatabaseName { get; set; }
    }
}