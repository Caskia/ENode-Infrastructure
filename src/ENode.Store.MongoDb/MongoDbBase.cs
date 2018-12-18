using ECommon.Utilities;
using ENode.Configurations;
using MongoDB.Driver;
using System;

namespace ENode.Store.MongoDb
{
    public abstract class MongoDbBase<TEntity>
    {
        private const string EventCollectionNameFormat = "{0}_{1}";
        private const string EventSingleCollectionNameFormat = "{0}";
        private readonly IShardableCollectionConfiguration _collectionConfiguration;
        private readonly IMongoDbProvider _databaseProvider;

        public MongoDbBase(
            IShardableCollectionConfiguration collectionConfiguration,
            IMongoDbProvider databaseProvider
            )
        {
            _collectionConfiguration = collectionConfiguration;
            _databaseProvider = databaseProvider;

            Ensure.NotNull(_collectionConfiguration.EntityName, nameof(_collectionConfiguration.EntityName));
            Ensure.Positive(_collectionConfiguration.ShardCount, nameof(_collectionConfiguration.ShardCount));

            CreateIndex();
        }

        public IMongoDatabase Database
        {
            get
            {
                return _databaseProvider.GetDatabase();
            }
        }

        public string EntityName
        {
            get
            {
                return _collectionConfiguration.EntityName;
            }
        }

        public int ShardCount
        {
            get
            {
                return _collectionConfiguration.ShardCount;
            }
        }

        public abstract void EnsureIndex(string collectionName);

        public IMongoCollection<TEntity> GetCollection(string aggregateRootId)
        {
            return Database.GetCollection<TEntity>(GetCollectionName(aggregateRootId));
        }

        public string GetCollectionName(string aggregateRootId)
        {
            var singleCollectionName = EntityName.EndsWith("s") ? EntityName : (EntityName + "s");

            if (ShardCount <= 1)
            {
                return string.Format(EventSingleCollectionNameFormat, singleCollectionName);
            }

            var collectionNumber = GetCollectionNumber(aggregateRootId);

            return string.Format(EventCollectionNameFormat, singleCollectionName, collectionNumber);
        }

        #region Private Methods

        private void CreateIndex()
        {
            var singleCollectionName = EntityName.EndsWith("s") ? EntityName : (EntityName + "s");

            if (ShardCount <= 1)
            {
                var collectionName = string.Format(EventSingleCollectionNameFormat, singleCollectionName);
                EnsureIndex(collectionName);
            }
            else
            {
                for (int i = 0; i < ShardCount; i++)
                {
                    var collectionName = string.Format(EventCollectionNameFormat, singleCollectionName, i);
                    EnsureIndex(collectionName);
                }
            }
        }

        private int GetCollectionNumber(string aggregateRootId)
        {
            int hash = 23;
            foreach (char c in aggregateRootId)
            {
                hash = (hash << 5) - hash + c;
            }
            if (hash < 0)
            {
                hash = Math.Abs(hash);
            }
            return hash % _collectionConfiguration.ShardCount;
        }

        #endregion Private Methods
    }
}