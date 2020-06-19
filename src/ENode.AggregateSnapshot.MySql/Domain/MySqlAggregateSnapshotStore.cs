using Dapper;
using ECommon.Dapper;
using ENode.AggregateSnapshot.Models;
using ENode.AggregateSnapshot.Repositories;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class MySqlAggregateSnapshotStore : IAggregateSnapshotStore
    {
        private const string InsertOrUpdateSnapshotSql = "INSERT INTO {0} (AggregateRootId, AggregateRootTypeName, CreationTime, Payload, Version) VALUES (@AggregateRootId, @AggregateRootTypeName, @CreationTime, @Payload, @Version) ON DUPLICATE KEY UPDATE ModificationTime=@ModificationTime, Payload=@Payload, Version=@Version";
        private readonly ISnapshotRepository _snapshotRepository;

        public MySqlAggregateSnapshotStore(ISnapshotRepository snapshotRepository)
        {
            _snapshotRepository = snapshotRepository;
        }

        public async Task CreateOrUpdateSnapshotPayloadAsync(string aggregateRootId, string aggregateRootTypeName, int publishedVersion, string payload)
        {
            var snapshot = new Snapshot()
            {
                CreationTime = DateTime.UtcNow,
                ModificationTime = DateTime.UtcNow,
                AggregateRootId = aggregateRootId,
                AggregateRootTypeName = aggregateRootTypeName,
                Version = publishedVersion,
                SerializedPayload = payload,
            };
            var sql = string.Format(InsertOrUpdateSnapshotSql, _snapshotRepository.GetTableName(snapshot.AggregateRootId));

            using (var connection = _snapshotRepository.GetConnection())
            {
                await connection.OpenAsync();
                await connection.ExecuteAsync(sql, snapshot);
            }
        }

        public async Task<string> GetSnapshotPayloadAsync(string aggregateRootId)
        {
            using (var connection = _snapshotRepository.GetConnection())
            {
                await connection.OpenAsync();

                var result = await connection.QueryListAsync<Snapshot>(new { AggregateRootId = aggregateRootId }, _snapshotRepository.GetTableName(aggregateRootId));
                return result.FirstOrDefault()?.SerializedPayload;
            }
        }
    }
}