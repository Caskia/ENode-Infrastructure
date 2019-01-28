using ECommon.Components;
using ENode.AggregateSnapshot.Configurations;
using ENode.AggregateSnapshot.Repositories;
using ENode.AggregateSnapshot.Serializers;
using ENode.Configurations;
using ENode.Domain;

namespace ENode.AggregateSnapshot
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Initialize the MySqlAggregateSnapshotter with option setting.
        /// </summary>
        public static ENodeConfiguration InitializeMySqlAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration,
            string connectionString,
            string tableName = "AggregateSnapshot",
            int tableCount = 1,
            int versionInterval = 50
            )
        {
            var aggregateSnapshotConfiguration = ObjectContainer.Resolve<IAggregateSnapshotConfiguration>();
            aggregateSnapshotConfiguration.VersionInterval = versionInterval;

            var snapshotMySqlConfiguration = ObjectContainer.Resolve<ISnapshotMySqlConfiguration>();
            snapshotMySqlConfiguration.ConnectionString = connectionString;
            snapshotMySqlConfiguration.TableName = tableName;
            snapshotMySqlConfiguration.TableCount = tableCount;

            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MySqlAggregateSnapshotter as the IAggregateSnapshotter.
        /// </summary>
        public static ENodeConfiguration UseMySqlAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotMySqlConfiguration, SnapshotMySqlConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotRepository, SnapshotRepository>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotConfiguration, AggregateSnapshotConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotSerializer, JsonAggregateSnapshotSerializer>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotter, MySqlAggregateSnapshotter>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISavableAggregateSnapshotter, SavableMySqlAggregateSnapshotter>();
            return eNodeConfiguration;
        }
    }
}