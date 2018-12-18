using ENode.AggregateSnapshot.Configuration;
using ENode.AggregateSnapshot.Models;
using ENode.Store.MongoDb;
using MongoDB.Driver;
using System.Collections.Generic;

namespace ENode.AggregateSnapshot.Collections
{
    public class SnapshotCollection : MongoDbBase<Snapshot>, ISnapshotCollection
    {
        public SnapshotCollection(
            ISnapshotCollectionConfiguration collectionConfiguration,
            ISnapshotMongoDbProvider databaseProvider
            ) : base(collectionConfiguration, databaseProvider)
        {
        }

        public override void EnsureIndex(string collectionName)
        {
            Database.GetCollection<Snapshot>(collectionName).Indexes.CreateMany(
                new List<CreateIndexModel<Snapshot>>()
                {
                    new CreateIndexModel<Snapshot>(Builders<Snapshot>.IndexKeys.Ascending(f => f.AggregateRootId),
                    new CreateIndexOptions()
                    {
                        Unique = true
                    })
                });
        }
    }
}