using ENode.AggregateSnapshot.Configuration;
using MongoDB.Driver;

namespace ENode.AggregateSnapshot
{
    public class MongoDbProvider : IMongoDbProvider
    {
        private readonly IMongoDbConfiguration _configuration;
        private MongoClient _mongoClient;

        public MongoDbProvider(IMongoDbConfiguration configuration)
        {
            _configuration = configuration;
        }

        public IMongoClient GetClient()
        {
            if (_mongoClient == null)
            {
                _mongoClient = new MongoClient(_configuration.ConnectionString /*+ "?maxPoolSize=100"*/);
            }
            return _mongoClient;
        }

        public IMongoDatabase GetDatabase()
        {
            return GetClient().GetDatabase(_configuration.DatabaseName);
        }
    }
}