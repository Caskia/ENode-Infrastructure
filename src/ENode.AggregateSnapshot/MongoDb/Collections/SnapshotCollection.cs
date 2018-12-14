using ENode.AggregateSnapshot.Models;
using MongoDB.Driver;
using System.Collections.Generic;

namespace ENode.AggregateSnapshot.Collections
{
    public class SnapshotCollection : MongoDbConnection
    {
        public SnapshotCollection(
            MongoDbConfiguration configuration,
            string storeEntityName = "AggregateSnapshot",
            int collectionCount = 1
            )
            : base(configuration, storeEntityName, collectionCount)
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

        public IMongoCollection<Snapshot> GetCollection(string aggregateRootId)
        {
            return GetCollection<Snapshot>(aggregateRootId);
        }
    }
}