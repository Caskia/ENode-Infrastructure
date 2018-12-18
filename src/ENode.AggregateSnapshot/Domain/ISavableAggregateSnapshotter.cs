using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public interface ISavableAggregateSnapshotter : IAggregateSnapshotter
    {
        Task SaveSnapshotAsync(IAggregateRoot aggregateRoot, Type aggregateRootType);
    }
}