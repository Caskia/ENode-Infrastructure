namespace ENode.Configurations
{
    public class MongoDbConfiguration : IMongoDbConfiguration
    {
        public string ConnectionString { get; set; }

        public string DatabaseName { get; set; }
    }
}