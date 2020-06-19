using ECommon.Components;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Configurations;
using ENode.Configurations;

namespace ENode.AggregateSnapshot
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Initialize the MongoDbAggregateSnapshotter with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMongoDbAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration,
            MongoDbConfiguration dbConfiguration,
            string storeEntityName = "AggregateSnapshot",
            int collectionCount = 1,
            int versionInterval = 50,
            int batchStoreIntervalMilliseconds = 1000,
            int batchStoreMaximumCumulativeCount = 100
            )
        {
            eNodeConfiguration.InitializeAggregateSnapshotter(versionInterval, batchStoreIntervalMilliseconds, batchStoreMaximumCumulativeCount);

            var mongoDbConfiguration = ObjectContainer.Resolve<ISnapshotMongoDbConfiguration>();
            mongoDbConfiguration.ConnectionString = dbConfiguration.ConnectionString;
            mongoDbConfiguration.DatabaseName = dbConfiguration.DatabaseName;

            var collectionConfiguration = ObjectContainer.Resolve<ISnapshotCollectionConfiguration>();
            collectionConfiguration.EntityName = storeEntityName;
            collectionConfiguration.ShardCount = collectionCount;

            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbAggregateSnapshotter as the IAggregateSnapshotter.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.UseAggregateSnapshotter();

            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotMongoDbConfiguration, SnapshotMongoDbConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotMongoDbProvider, SnapshotMongoDbProvider>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotCollectionConfiguration, SnapshotCollectionConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotCollection, SnapshotCollection>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotStore, MongoDbAggregateSnapshotStore>();
            return eNodeConfiguration;
        }
    }
}