using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class DefaultAggregateSnapshotter : IAggregateSnapshotter
    {
        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly IAggregateSnapshotStore _aggregateSnapshotStore;

        public DefaultAggregateSnapshotter
        (
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            IAggregateSnapshotStore aggregateSnapshotStore

        )
        {
            _aggregateSnapshotSerializer = aggregateSnapshotSerializer;
            _aggregateSnapshotStore = aggregateSnapshotStore;
        }

        public async Task<IAggregateRoot> RestoreFromSnapshotAsync(Type aggregateRootType, string aggregateRootId)
        {
            var payload = await _aggregateSnapshotStore.GetSnapshotPayloadAsync(aggregateRootId);

            if (payload == null)
            {
                return null;
            }

            return _aggregateSnapshotSerializer.Deserialize(payload, aggregateRootType) as IAggregateRoot;
        }
    }
}