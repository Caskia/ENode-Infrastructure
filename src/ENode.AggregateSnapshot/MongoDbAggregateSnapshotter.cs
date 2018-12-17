using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Models;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MongoDbAggregateSnapshotter : ISavableAggregateSnapshotter
    {
        #region Private Variables

        private IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private ILogger _logger;
        private SnapshotCollection _snapshotCollection;
        private ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Public Methods

        public MongoDbAggregateSnapshotter Initialize(
           MongoDbConfiguration configuration,
           string storeEntityName = "AggregateSnapshot",
           int collectionCount = 1
           )
        {
            _aggregateSnapshotSerializer = ObjectContainer.Resolve<IAggregateSnapshotSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();

            _snapshotCollection = new SnapshotCollection(configuration, storeEntityName, collectionCount);

            return this;
        }

        public async Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId)
        {
            var snapshot = await _snapshotCollection
                 .GetCollection(aggregateRootId)
                 .Find(s => s.AggregateRootId == aggregateRootId)
                 .FirstOrDefaultAsync();

            if (snapshot == null)
            {
                return null;
            }

            return _aggregateSnapshotSerializer.Deserialize(snapshot.Payload, aggregateRootType) as IAggregateRoot;
        }

        public async Task SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType)
        {
            if (aggregateRoot == null)
            {
                throw new ArgumentNullException(nameof(aggregateRoot));
            }

            var aggregateRootJson = _aggregateSnapshotSerializer.Serialize(aggregateRoot);
            var aggregateRootTypeName = _typeNameProvider.GetTypeName(aggregateRootType);
            var snapshot = new Snapshot()
            {
                Id = ObjectId.GenerateNewId(),
                AggregateRootId = aggregateRoot.UniqueId,
                AggregateRootTypeName = aggregateRootTypeName,
                Version = aggregateRoot.Version,
                Payload = aggregateRootJson
            };

            var filter = Builders<Snapshot>
               .Filter
               .Eq(s => s.AggregateRootId, snapshot.AggregateRootId);

            var update = Builders<Snapshot>
               .Update
               .Set(s => s.Payload, snapshot.Payload)
               .SetOnInsert(s => s.Id, snapshot.Id)
               .SetOnInsert(s => s.AggregateRootId, snapshot.AggregateRootId)
               .SetOnInsert(s => s.AggregateRootTypeName, snapshot.AggregateRootTypeName);

            await _snapshotCollection
                .GetCollection(aggregateRoot.UniqueId)
                .UpdateOneAsync(filter, update, new UpdateOptions()
                {
                    IsUpsert = true
                });
        }

        #endregion Public Methods
    }
}