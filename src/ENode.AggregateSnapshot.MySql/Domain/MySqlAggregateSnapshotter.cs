using ECommon.Dapper;
using ECommon.Logging;
using ENode.AggregateSnapshot.Models;
using ENode.AggregateSnapshot.Repositories;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MySqlAggregateSnapshotter : IAggregateSnapshotter
    {
        #region Private Variables

        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly ILogger _logger;
        private readonly ISnapshotRepository _snapshotRepository;
        private readonly ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public MySqlAggregateSnapshotter(
            ISnapshotRepository snapshotRepository,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            ILoggerFactory loggerFactory,
            ITypeNameProvider typeNameProvider
            )
        {
            _snapshotRepository = snapshotRepository;
            _aggregateSnapshotSerializer = aggregateSnapshotSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
            _typeNameProvider = typeNameProvider;
        }

        #endregion Ctor

        #region Public Methods

        public async Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId)
        {
            var snapshot = default(Snapshot);

            using (var connection = _snapshotRepository.GetConnection())
            {
                var result = await connection.QueryListAsync<Snapshot>(new { AggregateRootId = aggregateRootId }, _snapshotRepository.GetTableName(aggregateRootId));
                snapshot = result.SingleOrDefault();
            }

            if (snapshot == null)
            {
                return null;
            }

            return _aggregateSnapshotSerializer.Deserialize(snapshot.SerializedPayload, aggregateRootType) as IAggregateRoot;
        }

        #endregion Public Methods
    }
}