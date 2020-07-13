using ENode.AggregateSnapshot.Configurations;
using MySqlConnector;
using System;

namespace ENode.AggregateSnapshot.Repositories
{
    public class SnapshotRepository : ISnapshotRepository
    {
        private const string SnapshotSingleTableNameFormat = "`{0}`";
        private const string SnapshotTableNameFormat = "`{0}_{1}`";
        private readonly ISnapshotMySqlConfiguration _snapshotMySqlConfiguration;

        public SnapshotRepository(ISnapshotMySqlConfiguration snapshotMySqlConfiguration)
        {
            _snapshotMySqlConfiguration = snapshotMySqlConfiguration;
        }

        public MySqlConnection GetConnection()
        {
            return new MySqlConnection(_snapshotMySqlConfiguration.ConnectionString);
        }

        public int GetTableIndex(string aggregateRootId)
        {
            int hash = 23;
            foreach (char c in aggregateRootId)
            {
                hash = (hash << 5) - hash + c;
            }
            if (hash < 0)
            {
                hash = Math.Abs(hash);
            }
            return hash % _snapshotMySqlConfiguration.TableCount;
        }

        public string GetTableName(string aggregateRootId)
        {
            if (_snapshotMySqlConfiguration.TableCount <= 1)
            {
                return string.Format(SnapshotSingleTableNameFormat, _snapshotMySqlConfiguration.TableName);
            }

            var tableIndex = GetTableIndex(aggregateRootId);

            return string.Format(SnapshotTableNameFormat, _snapshotMySqlConfiguration.TableName, tableIndex);
        }
    }
}