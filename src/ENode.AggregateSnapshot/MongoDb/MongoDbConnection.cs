using ECommon.Utilities;
using MongoDB.Driver;
using System;

namespace ENode.AggregateSnapshot
{
    public abstract class MongoDbConnection
    {
        private const string EventCollectionNameFormat = "{0}_{1}";
        private const string EventSingleCollectionNameFormat = "{0}";

        #region Private Variables

        private readonly int _collectionCount;

        private readonly MongoDbConfiguration _configuration;

        private readonly MongoDbProvider _mongoDbProvider;

        private readonly string _storeEntityName;

        #endregion Private Variables

        #region Ctor

        public MongoDbConnection(
            MongoDbConfiguration configuration,
            string storeEntityName,
            int collectionCount
            )
        {
            _configuration = configuration;
            _storeEntityName = storeEntityName;
            _collectionCount = collectionCount;

            Ensure.NotNull(_configuration, "_configuration");
            Ensure.NotNull(_storeEntityName, "_storeEntityName");
            Ensure.Positive(_collectionCount, "_collectionCount");

            _mongoDbProvider = new MongoDbProvider(_configuration);

            CreateIndex();
        }

        #endregion Ctor

        #region Public Methods

        public IMongoDatabase Database
        {
            get { return _mongoDbProvider.GetDatabase(); }
        }

        public abstract void EnsureIndex(string collectionName);

        public string GetCollectionName(string aggregateRootId)
        {
            var singleCollectionName = _storeEntityName.EndsWith("s") ? _storeEntityName : (_storeEntityName + "s");

            if (_collectionCount <= 1)
            {
                return string.Format(EventSingleCollectionNameFormat, singleCollectionName);
            }

            var collectionNumber = GetCollectionNumber(aggregateRootId);

            return string.Format(EventCollectionNameFormat, singleCollectionName, collectionNumber);
        }

        #endregion Public Methods

        #region Protect Methods

        protected IMongoCollection<TEntity> GetCollection<TEntity>(string aggregateRootId)
        {
            return Database.GetCollection<TEntity>(GetCollectionName(aggregateRootId));
        }

        #endregion Protect Methods

        #region Private Methods

        private void CreateIndex()
        {
            var singleCollectionName = _storeEntityName.EndsWith("s") ? _storeEntityName : (_storeEntityName + "s");

            if (_collectionCount <= 1)
            {
                var collectionName = string.Format(EventSingleCollectionNameFormat, singleCollectionName);
                EnsureIndex(collectionName);
            }
            else
            {
                for (int i = 0; i < _collectionCount; i++)
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
            return hash % _collectionCount;
        }

        #endregion Private Methods
    }
}