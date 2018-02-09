using ENode.EventStore.MongoDb.Models;
using MongoDB.Driver;

namespace ENode.EventStore.MongoDb.Collections
{
    public class PublishedVersionStoreCollection : MongoDbConnection
    {
        public PublishedVersionStoreCollection(
            MongoDbConfiguration configuration,
            string storeEntityName = "PublishedVersion",
            int collectionCount = 1
            )
            : base(configuration, storeEntityName, collectionCount)
        {
        }

        public override void EnsureIndex(string collectionName)
        {
            Database.GetCollection<PublishedVersion>(collectionName).Indexes.CreateOne(
                            Builders<PublishedVersion>
                            .IndexKeys
                            .Ascending(f => f.ProcessorName)
                            .Ascending(f => f.AggregateRootId)
                            .Ascending(f => f.Version),
                            new CreateIndexOptions()
                            {
                                Unique = true
                            });
        }

        public IMongoCollection<PublishedVersion> GetCollection(string aggregateRootId)
        {
            return GetCollection<PublishedVersion>(aggregateRootId);
        }
    }
}