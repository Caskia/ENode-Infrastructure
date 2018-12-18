using MongoDB.Driver;

namespace ENode.Store.MongoDb
{
    public interface IMongoDbProvider
    {
        IMongoClient GetClient();

        IMongoDatabase GetDatabase();
    }
}