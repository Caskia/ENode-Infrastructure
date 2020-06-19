using ENode.Domain;
using System;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public interface IAggregateSnapshotSaver
    {
        Task SaveAsync(object aggregateRootId, Type aggregateRootType, int publishedVersion);

        Task SaveAsync(IAggregateRoot aggregateRoot, Type aggregateRootType, int publishedVersion);
    }
}