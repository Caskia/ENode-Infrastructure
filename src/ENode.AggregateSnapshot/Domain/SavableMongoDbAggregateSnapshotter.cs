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
    public class SavableMongoDbAggregateSnapshotter : ISavableAggregateSnapshotter
    {
        #region Private Variables

        private readonly IAggregateSnapshotConfiguration _aggregateSnapshotConfiguration;
        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly ILogger _logger;
        private readonly IRepository _repository;
        private readonly ISnapshotCollection _snapshotCollection;
        private readonly ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public SavableMongoDbAggregateSnapshotter(
            IAggregateSnapshotConfiguration aggregateSnapshotConfiguration,
            ISnapshotCollection snapshotCollection,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            IRepository repository,
            ILoggerFactory loggerFactory,
            ITypeNameProvider typeNameProvider
            )
        {
            _aggregateSnapshotConfiguration = aggregateSnapshotConfiguration;
            _snapshotCollection = snapshotCollection;
            _aggregateSnapshotSerializer = aggregateSnapshotSerializer;
            _repository = repository;
            _logger = loggerFactory.Create(GetType().FullName);
            _typeNameProvider = typeNameProvider;
        }

        #endregion Ctor

        #region Public Methods

        public async Task SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType, int publishedVersion)
        {
            if (aggregateRoot == null)
            {
                throw new ArgumentNullException(nameof(aggregateRoot));
            }

            if (publishedVersion % _aggregateSnapshotConfiguration.VersionInterval != 0)
            {
                return;
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

        public async Task SaveSnapshotAsync(object aggregateRootId, Type aggregateRootType, int publishedVersion)
        {
            if (aggregateRootId == null)
            {
                throw new ArgumentNullException("aggregateRootId");
            }

            var aggregateRoot = await _repository.GetAsync(aggregateRootType, aggregateRootId);
            if (aggregateRoot != null)
            {
                await SaveSnapshotAsync(aggregateRoot, aggregateRootType, publishedVersion);
            }
        }

        #endregion Public Methods
    }
}