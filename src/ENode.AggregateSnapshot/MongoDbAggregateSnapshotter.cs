using ECommon.Components;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Models;
using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MongoDbAggregateSnapshotter : IAggregateSnapshotter
    {
        #region Private Variables

        private IJsonSerializer _jsonSerializer;
        private ILogger _logger;
        private SnapshotCollection _snapshotCollection;

        #endregion Private Variables

        #region Public Methods

        public MongoDbAggregateSnapshotter Initialize(
           MongoDbConfiguration configuration,
           string storeEntityName = "AggregateSnapshot",
           int collectionCount = 1
           )
        {
            _jsonSerializer = ObjectContainer.Resolve<IJsonSerializer>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);

            _snapshotCollection = new SnapshotCollection(configuration, storeEntityName, collectionCount);

            return this;
        }

        public Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId)
        {
            throw new NotImplementedException();
        }

        public Task<Snapshot> SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType)
        {
            throw new NotImplementedException();
        }

        #endregion Public Methods
    }
}