using ECommon.Components;
using ENode.Configurations;
using ENode.Eventing;
using ENode.EventStore.MongoDb.Collections;
using ENode.EventStore.MongoDb.Configurations;
using ENode.Infrastructure;

namespace ENode.EventStore.MongoDb
{
    public static class ENodeExtensions
    {
        /// <summary>
        /// Initialize the MongoDbEventStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMongoDbEventStore(this ENodeConfiguration eNodeConfiguration,
            MongoDbConfiguration dbConfiguration,
            string storeEntityName = "EventStream",
            int collectionCount = 1)
        {
            var mongoDbConfiguration = ObjectContainer.Resolve<IEventStreamMongoDbConfiguration>();
            mongoDbConfiguration.ConnectionString = dbConfiguration.ConnectionString;
            mongoDbConfiguration.DatabaseName = dbConfiguration.DatabaseName;

            var collectionConfiguration = ObjectContainer.Resolve<IEventStreamCollectionConfiguration>();
            collectionConfiguration.EntityName = storeEntityName;
            collectionConfiguration.ShardCount = collectionCount;

            return eNodeConfiguration;
        }

        /// <summary>
        /// Initialize the MongoDbPublishedVersionStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMongoDbPublishedVersionStore(this ENodeConfiguration eNodeConfiguration,
            MongoDbConfiguration dbConfiguration,
            string storeEntityName = "PublishedVersion",
            int collectionCount = 1)
        {
            var mongoDbConfiguration = ObjectContainer.Resolve<IPublishedVersionMongoDbConfiguration>();
            mongoDbConfiguration.ConnectionString = dbConfiguration.ConnectionString;
            mongoDbConfiguration.DatabaseName = dbConfiguration.DatabaseName;

            var collectionConfiguration = ObjectContainer.Resolve<IPublishedVersionCollectionConfiguration>();
            collectionConfiguration.EntityName = storeEntityName;
            collectionConfiguration.ShardCount = collectionCount;

            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStreamMongoDbConfiguration, EventStreamMongoDbConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStreamMongoDbProvider, EventStreamMongoDbProvider>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStreamCollectionConfiguration, EventStreamCollectionConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStreamCollection, EventStreamCollection>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, MongoDbEventStore>();
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionMongoDbConfiguration, PublishedVersionMongoDbConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionMongoDbProvider, PublishedVersionMongoDbProvider>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionCollectionConfiguration, PublishedVersionCollectionConfiguration>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionCollection, PublishedVersionCollection>();
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, MongoDbPublishedVersionStore>();
            return eNodeConfiguration;
        }
    }
}