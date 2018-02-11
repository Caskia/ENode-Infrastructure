using ECommon.Components;
using ENode.Configurations;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.EventStore.MongoDb
{
    public static class ENodeExtensions
    {
        /// <summary>Initialize the MongoDbEventStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMySqlEventStore(this ENodeConfiguration eNodeConfiguration,
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

        /// <summary>Initialize the MongoDbPublishedVersionStore with option setting.
        /// </summary>
        /// <param name="eNodeConfiguration"></param>
        /// <param name="mongoDbConfiguration"></param>
        /// <param name="storeEntityName"></param>
        /// <param name="collectionCount"></param>
        /// <returns></returns>
        public static ENodeConfiguration InitializeMySqlPublishedVersionStore(this ENodeConfiguration eNodeConfiguration,
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

        /// <summary>Use the MongoDbEventStore as the IEventStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlEventStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IEventStore, MongoDbEventStore>();
            return eNodeConfiguration;
        }

        /// <summary>Use the MongoDbPublishedVersionStore as the IPublishedVersionStore.
        /// </summary>
        /// <returns></returns>
        public static ENodeConfiguration UseMySqlPublishedVersionStore(this ENodeConfiguration eNodeConfiguration)
        {
            eNodeConfiguration.GetCommonConfiguration().SetDefault<IPublishedVersionStore, MongoDbPublishedVersionStore>();
            return eNodeConfiguration;
        }
    }
}