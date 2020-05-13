using ECommon.Components;
using ENode.AggregateSnapshot.Configurations;
using ENode.AggregateSnapshot.Serializers;
using ENode.Configurations;
using ENode.Domain;

namespace ENode.AggregateSnapshot
{
    public static class ENodeExtensions
    {
        public static ENodeConfiguration InitializeAggregateSnapshotter
        (
            this ENodeConfiguration eNodeConfiguration,
            int versionInterval = 50,
            int batchStoreIntervalMilliseconds = 1000,
            int batchStoreMaximumCumulativeCount = 100
        )
        {
            var aggregateSnapshotConfiguration = ObjectContainer.Resolve<IAggregateSnapshotConfiguration>();
            aggregateSnapshotConfiguration.VersionInterval = versionInterval;
            aggregateSnapshotConfiguration.BatchStoreIntervalMilliseconds = batchStoreIntervalMilliseconds;
            aggregateSnapshotConfiguration.BatchStoreMaximumCumulativeCount = batchStoreMaximumCumulativeCount;

            return eNodeConfiguration;
        }

        public static ENodeConfiguration UseAggregateSnapshotter(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotConfiguration, AggregateSnapshotConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotSerializer, JsonAggregateSnapshotSerializer>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotter, DefaultAggregateSnapshotter>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IAggregateSnapshotSaver, DefaultAggregateSnapshotSaver>();
            return eNodeConfiguration;
        }
    }
}