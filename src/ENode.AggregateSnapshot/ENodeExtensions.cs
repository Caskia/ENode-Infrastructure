using ECommon.Components;
using ENode.AggregateSnapshot.Collections;
using ENode.AggregateSnapshot.Configuration;
using ENode.AggregateSnapshot.Serializers;
using ENode.Configurations;
using ENode.Domain;

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
            int collectionCount = 1)
        {
            var mongoDbConfiguration = ObjectContainer.Resolve<IMongoDbConfiguration>();
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
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IMongoDbConfiguration, MongoDbConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IMongoDbProvider, MongoDbProvider>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotCollectionConfiguration, SnapshotCollectionConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISnapshotCollection, SnapshotCollection>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotSerializer, JsonAggregateSnapshotSerializer>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotter, MongoDbAggregateSnapshotter>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<ISavableAggregateSnapshotter, MongoDbAggregateSnapshotter>();
            return eNodeConfiguration;
        }
    }
}