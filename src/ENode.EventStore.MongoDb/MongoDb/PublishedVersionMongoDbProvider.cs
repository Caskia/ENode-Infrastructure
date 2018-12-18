using ENode.EventStore.MongoDb.Configurations;
using ENode.Store.MongoDb;

namespace ENode.EventStore.MongoDb
{
    public class PublishedVersionMongoDbProvider : MongoDbProvider, IPublishedVersionMongoDbProvider
    {
        public PublishedVersionMongoDbProvider(IPublishedVersionMongoDbConfiguration configuration) : base(configuration)
        {
        }
    }
}