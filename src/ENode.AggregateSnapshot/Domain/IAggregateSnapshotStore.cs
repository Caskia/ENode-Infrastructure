using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public interface IAggregateSnapshotStore
    {
        Task CreateOrUpdateSnapshotPayloadAsync(string aggregateRootId, string aggregateRootTypeName, int publishedVersion, string payload);

        Task<string> GetSnapshotPayloadAsync(string aggregateRootId);
    }
}