using ECommon.Components;
using ENode.AggregateSnapshot.Configurations;
using ENode.AggregateSnapshot.Repositories;
using ENode.Configurations;

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
            int versionInterval = 50,
            int batchStoreIntervalSeconds = 1000,
            int batchStoreMaximumCumulativeCount = 100
            )
        {
            eNodeConfiguration.InitializeAggregateSnapshotter(versionInterval, batchStoreIntervalSeconds, batchStoreMaximumCumulativeCount);

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
            eNodeConfiguration.UseAggregateSnapshotter();

            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotMySqlConfiguration, SnapshotMySqlConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotRepository, SnapshotRepository>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotStore, MySqlAggregateSnapshotStore>();
            return eNodeConfiguration;
        }
    }
}