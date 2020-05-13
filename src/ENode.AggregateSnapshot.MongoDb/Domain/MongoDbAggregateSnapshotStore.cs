using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Models;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MongoDbAggregateSnapshotStore : IAggregateSnapshotStore
    {
        private readonly ISnapshotCollection _snapshotCollection;

        public MongoDbAggregateSnapshotStore(ISnapshotCollection snapshotCollection)
        {
            _snapshotCollection = snapshotCollection;
        }

        public async Task CreateOrUpdateSnapshotPayloadAsync(string aggregateRootId, string aggregateRootTypeName, int publishedVersion, string payload)
        {
            var snapshot = new Snapshot()
            {
                Id = ObjectId.GenerateNewId(),
                CreationTime = DateTime.UtcNow,
                ModificationTime = DateTime.UtcNow,
                AggregateRootId = aggregateRootId,
                AggregateRootTypeName = aggregateRootTypeName,
                Version = publishedVersion,
                Payload = payload,
            };

            var filter = Builders<Snapshot>
                .Filter
                .Eq(s => s.AggregateRootId, snapshot.AggregateRootId);

            var update = Builders<Snapshot>
               .Update
               .Set(s => s.ModificationTime, snapshot.ModificationTime)
               .Set(s => s.Version, snapshot.Version)
               .Set(s => s.Payload, snapshot.Payload)
               .SetOnInsert(s => s.Id, snapshot.Id)
               .SetOnInsert(s => s.CreationTime, snapshot.CreationTime)
               .SetOnInsert(s => s.AggregateRootId, snapshot.AggregateRootId)
               .SetOnInsert(s => s.AggregateRootTypeName, snapshot.AggregateRootTypeName);

            await _snapshotCollection
                .GetCollection(aggregateRootId)
                .UpdateOneAsync(filter, update, new UpdateOptions()
                {
                    IsUpsert = true
                });
        }

        public async Task<string> GetSnapshotPayloadAsync(string aggregateRootId)
        {
            var snapshot = await _snapshotCollection
                             .GetCollection(aggregateRootId)
                             .Find(s => s.AggregateRootId == aggregateRootId)
                             .FirstOrDefaultAsync();

            return snapshot?.Payload;
        }
    }
}