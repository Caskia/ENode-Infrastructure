using ECommon.Logging;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Configurations;
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

        private IAggregateSnapshotConfiguration _aggregateSnapshotConfiguration;
        private IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private ILogger _logger;
        private ISnapshotCollection _snapshotCollection;
        private ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public MongoDbAggregateSnapshotter(
            IAggregateSnapshotConfiguration aggregateSnapshotConfiguration,
            ISnapshotCollection snapshotCollection,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            ILoggerFactory loggerFactory,
            ITypeNameProvider typeNameProvider
            )
        {
            _aggregateSnapshotConfiguration = aggregateSnapshotConfiguration;
            _snapshotCollection = snapshotCollection;
            _aggregateSnapshotSerializer = aggregateSnapshotSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
            _typeNameProvider = typeNameProvider;
        }

        #endregion Ctor

        #region Public Methods

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
                CreationTime = DateTime.UtcNow,
                ModificationTime = DateTime.UtcNow,
                AggregateRootId = aggregateRoot.UniqueId,
                AggregateRootTypeName = aggregateRootTypeName,
                Version = aggregateRoot.Version,
                Payload = aggregateRootJson,
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
                .GetCollection(aggregateRoot.UniqueId)
                .UpdateOneAsync(filter, update, new UpdateOptions()
                {
                    IsUpsert = true
                });
        }

        #endregion Public Methods
    }
}