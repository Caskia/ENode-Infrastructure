using ECommon.Logging;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MongoDbAggregateSnapshotter : IAggregateSnapshotter
    {
        #region Private Variables

        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly ILogger _logger;
        private readonly ISnapshotCollection _snapshotCollection;
        private readonly ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public MongoDbAggregateSnapshotter(
            ISnapshotCollection snapshotCollection,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            ILoggerFactory loggerFactory,
            ITypeNameProvider typeNameProvider
            )
        {
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

        #endregion Public Methods
    }
}