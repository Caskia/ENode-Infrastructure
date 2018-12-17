using ECommon.Components;
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
            MongoDbConfiguration mongoDbConfiguration,
            string storeEntityName = "AggregateSnapshot",
            int collectionCount = 1)
        {
            ((MongoDbAggregateSnapshotter)ObjectContainer.Resolve<IAggregateSnapshotter>()).Initialize(
                mongoDbConfiguration,
                storeEntityName,
                collectionCount);
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbAggregateSnapshotter as the IAggregateSnapshotter.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotSerializer, JsonAggregateSnapshotSerializer>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotter, MongoDbAggregateSnapshotter>();
            return eNodeConfiguration;
        }
    }
}