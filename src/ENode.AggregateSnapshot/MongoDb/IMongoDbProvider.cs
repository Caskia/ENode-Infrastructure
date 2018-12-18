using MongoDB.Driver;

namespace ENode.AggregateSnapshot
{
    public interface IMongoDbProvider
    {
        IMongoClient GetClient();

        IMongoDatabase GetDatabase();
    }
}