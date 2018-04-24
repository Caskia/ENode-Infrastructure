using ECommon.Components;
using ENode.Configurations;
using ENode.Eventing;
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
            MongoDbConfiguration mongoDbConfiguration,
            string storeEntityName = "EventStream",
            int collectionCount = 1)
        {
            ((MongoDbEventStore)ObjectContainer.Resolve<IEventStore>()).Initialize(
                mongoDbConfiguration,
                storeEntityName,
                collectionCount);
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
            MongoDbConfiguration mongoDbConfiguration,
            string storeEntityName = "PublishedVersion",
            int collectionCount = 1)
        {
            ((MongoDbPublishedVersionStore)ObjectContainer.Resolve<IPublishedVersionStore>()).Initialize(
                mongoDbConfiguration,
                storeEntityName,
                collectionCount);
            return eNodeConfiguration;
        }

        /// <summary>
        /// Initialize the MongoDbPublishedVersionStoreSync with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMongoDbPublishedVersionStoreSync(this ENodeConfiguration eNodeConfiguration,
            MongoDbConfiguration mongoDbConfiguration,
            string storeEntityName = "PublishedVersion",
            int collectionCount = 1)
        {
            ((MongoDbPublishedVersionStoreSync)ObjectContainer.Resolve<IPublishedVersionStore>()).Initialize(
                mongoDbConfiguration,
                storeEntityName,
                collectionCount);
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, MongoDbEventStore>();
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, MongoDbPublishedVersionStore>();
            return eNodeConfiguration;
        }

        /// <summary>
        /// Use the MongoDbPublishedVersionStoreSync as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMongoDbPublishedVersionStoreSync(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, MongoDbPublishedVersionStoreSync>();
            return eNodeConfiguration;
        }
    }
}