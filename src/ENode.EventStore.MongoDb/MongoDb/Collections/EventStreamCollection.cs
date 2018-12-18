using ENode.EventStore.MongoDb.Configurations;
using ENode.EventStore.MongoDb.Models;
using ENode.Store.MongoDb;
using MongoDB.Driver;
using System.Collections.Generic;

namespace ENode.EventStore.MongoDb.Collections
{
    public class EventStreamCollection : MongoDbBase<EventStream>, IEventStreamCollection
    {
        public EventStreamCollection(
            IEventStreamCollectionConfiguration collectionConfiguration,
            IEventStreamMongoDbProvider databaseProvider
            ) : base(collectionConfiguration, databaseProvider)
        {
        }

        public override void EnsureIndex(string collectionName)
        {
            Database.GetCollection<EventStream>(collectionName).Indexes.CreateMany(
                new List<CreateIndexModel<EventStream>>()
                {
                    new CreateIndexModel<EventStream>(Builders<EventStream>.IndexKeys.Ascending(f => f.AggregateRootId).Ascending(f => f.Version),
                        new CreateIndexOptions()
                        {
                            Unique = true
                        }),
                    new CreateIndexModel<EventStream>(Builders<EventStream>.IndexKeys.Ascending(f => f.AggregateRootId).Ascending(f => f.CommandId),
                        new CreateIndexOptions()
                        {
                            Unique = true
                        })
                });
        }
    }
}