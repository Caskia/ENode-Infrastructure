using MySqlConnector;

namespace ENode.AggregateSnapshot.Repositories
{
    public interface ISnapshotRepository
    {
        MySqlConnection GetConnection();

        int GetTableIndex(string aggregateRootId);

        string GetTableName(string aggregateRootId);
    }
}