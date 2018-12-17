using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Models;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MongoDbAggregateSnapshotter : IAggregateSnapshotter
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

        public Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId)
        {
            throw new NotImplementedException();
        }

        public Task<Snapshot> SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType)
        {
            if (aggregateRoot == null)
            {
                throw new ArgumentNullException(nameof(aggregateRoot));
            }

            throw new NotImplementedException();
        }

        #endregion Public Methods
    }
}