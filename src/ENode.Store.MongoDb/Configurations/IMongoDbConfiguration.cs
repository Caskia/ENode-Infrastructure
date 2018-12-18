namespace ENode.Configurations
{
    public interface IMongoDbConfiguration
    {
        string ConnectionString { get; set; }

        string DatabaseName { get; set; }
    }
}