using ENode.EventStore.MongoDb.Models;
using MongoDB.Driver;

namespace ENode.EventStore.MongoDb.Collections
{
    public class EventStreamCollection : MongoDbConnection
    {
        public EventStreamCollection(
            MongoDbConfiguration configuration,
            string storeEntityName = "EventStream",
            int collectionCount = 1
            )
            : base(configuration, storeEntityName, collectionCount)
        {
        }

        public override void EnsureIndex(string collectionName)
        {
            Database.GetCollection<EventStream>(collectionName).Indexes.CreateOne(
                Builders<EventStream>
                .IndexKeys
                .Ascending(f => f.AggregateRootId)
                .Ascending(f => f.Version),
                new CreateIndexOptions()
                {
                    Unique = true
                });

            Database.GetCollection<EventStream>(collectionName).Indexes.CreateOne(
                Builders<EventStream>
                .IndexKeys
                .Ascending(f => f.AggregateRootId)
                .Ascending(f => f.CommandId),
                new CreateIndexOptions()
                {
                    Unique = true
                });
        }

        public IMongoCollection<EventStream> GetCollection(string aggregateRootId)
        {
            return GetCollection<EventStream>(aggregateRootId);
        }
    }
}