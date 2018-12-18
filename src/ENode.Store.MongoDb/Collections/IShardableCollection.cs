using MongoDB.Driver;

namespace ENode.Store.MongoDb.Collections
{
    public interface IShardableCollection<TEntity>
    {
        IMongoCollection<TEntity> GetCollection(string aggregateRootId);
    }
}