using ENode.EventStore.MongoDb.Models;
using ENode.Store.MongoDb.Collections;

namespace ENode.EventStore.MongoDb.Collections
{
    public interface IPublishedVersionCollection : IShardableCollection<PublishedVersion>
    {
    }
}