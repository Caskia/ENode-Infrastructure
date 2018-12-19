using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public interface ISavableAggregateSnapshotter
    {
        Task SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType, int publishedVersion);

        Task SaveSnapshotAsync(object aggregateRootId, Type aggregateRootType, int publishedVersion);
    }
}