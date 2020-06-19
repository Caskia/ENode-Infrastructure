namespace ENode.AggregateSnapshot.Configurations
{
    public interface IAggregateSnapshotConfiguration
    {
        int BatchStoreIntervalMilliseconds { get; set; }

        int BatchStoreMaximumCumulativeCount { get; set; }

        int VersionInterval { get; set; }
    }
}