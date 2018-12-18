using ENode.AggregateSnapshot.Models;
using ENode.Store.MongoDb.Collections;

namespace ENode.AggregateSnapshot.Collections
{
    public interface ISnapshotCollection : IShardableCollection<Snapshot>
    {
    }
}