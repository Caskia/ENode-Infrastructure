using System;

namespace ENode.AggregateSnapshot
{
    public class AggregateSnapshotStoreContext
    {
        public string AggregateRootId { get; set; }

        public Type AggregateRootType { get; set; }

        public string Payload { get; set; }

        public int PublishedVersion { get; set; }
    }
}