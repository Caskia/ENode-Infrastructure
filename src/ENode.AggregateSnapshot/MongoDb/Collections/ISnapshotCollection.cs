using ENode.AggregateSnapshot.Models;
using MongoDB.Driver;

namespace ENode.AggregateSnapshot.Collections
{
    public interface ISnapshotCollection
    {
        IMongoCollection<Snapshot> GetCollection(string aggregateRootId);
    }
}