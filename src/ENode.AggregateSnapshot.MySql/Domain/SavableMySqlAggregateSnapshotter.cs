using Dapper;
using DeepCopy;
using ECommon.Logging;
using ENode.AggregateSnapshot.Configurations;
using ENode.AggregateSnapshot.Models;
using ENode.AggregateSnapshot.Repositories;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class SavableMySqlAggregateSnapshotter : ISavableAggregateSnapshotter
    {
        #region Private Variables

        private const string InsertOrUpdateSnapshotSql = "INSERT INTO {0} (AggregateRootId, AggregateRootTypeName, CreationTime, Payload, Version) VALUES (@AggregateRootId, @AggregateRootTypeName, @CreationTime, @Payload, @Version) ON DUPLICATE KEY UPDATE ModificationTime=@ModificationTime, Payload=@Payload, Version=@Version";
        private readonly IAggregateSnapshotConfiguration _aggregateSnapshotConfiguration;
        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly ILogger _logger;
        private readonly IRepository _repository;
        private readonly ISnapshotRepository _snapshotRepository;
        private readonly ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public SavableMySqlAggregateSnapshotter(
            IAggregateSnapshotConfiguration aggregateSnapshotConfiguration,
            ISnapshotRepository snapshotRepository,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            IRepository repository,
            ILoggerFactory loggerFactory,
            ITypeNameProvider typeNameProvider
            )
        {
            _aggregateSnapshotConfiguration = aggregateSnapshotConfiguration;
            _snapshotRepository = snapshotRepository;
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

            var copiedAggregateRoot = DeepCopier.Copy(aggregateRoot);
            var aggregateRootJson = _aggregateSnapshotSerializer.Serialize(copiedAggregateRoot);
            var aggregateRootTypeName = _typeNameProvider.GetTypeName(aggregateRootType);
            var snapshot = new Snapshot()
            {
                CreationTime = DateTime.UtcNow,
                ModificationTime = DateTime.UtcNow,
                AggregateRootId = copiedAggregateRoot.UniqueId,
                AggregateRootTypeName = aggregateRootTypeName,
                Version = copiedAggregateRoot.Version,
                SerializedPayload = aggregateRootJson,
            };
            var sql = string.Format(InsertOrUpdateSnapshotSql, _snapshotRepository.GetTableName(snapshot.AggregateRootId));

            using (var connection = _snapshotRepository.GetConnection())
            {
                await connection.OpenAsync();
                await connection.ExecuteAsync(sql, snapshot);
            }
        }

        public async Task SaveSnapshotAsync(object aggregateRootId, Type aggregateRootType, int publishedVersion)
        {
            if (aggregateRootId == null)
            {
                throw new ArgumentNullException("aggregateRootId");
            }

            if (publishedVersion % _aggregateSnapshotConfiguration.VersionInterval != 0)
            {
                return;
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