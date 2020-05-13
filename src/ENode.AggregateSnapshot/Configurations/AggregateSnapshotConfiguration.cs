namespace ENode.AggregateSnapshot.Configurations
{
    public class AggregateSnapshotConfiguration : IAggregateSnapshotConfiguration
    {
        public int BatchStoreIntervalMilliseconds { get; set; }

        public int BatchStoreMaximumCumulativeCount { get; set; }

        public int VersionInterval { get; set; }
    }
}