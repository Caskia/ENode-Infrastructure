using ENode.EventStore.MongoDb.Configurations;
using ENode.EventStore.MongoDb.Models;
using ENode.Store.MongoDb;
using MongoDB.Driver;

namespace ENode.EventStore.MongoDb.Collections
{
    public class PublishedVersionCollection : MongoDbBase<PublishedVersion>, IPublishedVersionCollection
    {
        public PublishedVersionCollection(
            IPublishedVersionCollectionConfiguration collectionConfiguration,
            IPublishedVersionMongoDbProvider databaseProvider
            ) : base(collectionConfiguration, databaseProvider)
        {
        }

        public override void EnsureIndex(string collectionName)
        {
            Database.GetCollection<PublishedVersion>(collectionName).Indexes.CreateOne(
                new CreateIndexModel<PublishedVersion>(Builders<PublishedVersion>.IndexKeys.Ascending(f => f.ProcessorName).Ascending(f => f.AggregateRootId).Ascending(f => f.Version),
                    new CreateIndexOptions()
                    {
                        Unique = true
                    })
            );
        }
    }
}