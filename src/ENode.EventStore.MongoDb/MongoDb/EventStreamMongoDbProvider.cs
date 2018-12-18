using ENode.EventStore.MongoDb.Configurations;
using ENode.Store.MongoDb;

namespace ENode.EventStore.MongoDb
{
    public class EventStreamMongoDbProvider : MongoDbProvider, IEventStreamMongoDbProvider
    {
        public EventStreamMongoDbProvider(IEventStreamMongoDbConfiguration configuration) : base(configuration)
        {
        }
    }
}