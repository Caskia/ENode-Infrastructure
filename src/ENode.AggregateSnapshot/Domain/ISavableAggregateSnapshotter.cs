using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public interface ISavableAggregateSnapshotter
    {
        Task SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType);

        Task SaveSnapshotAsync(object aggregateRootId, Type aggregateRootType);
    }
}