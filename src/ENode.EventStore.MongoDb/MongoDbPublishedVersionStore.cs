using ECommon.IO;
using ECommon.Logging;
using ENode.AggregateSnapshot;
using ENode.EventStore.MongoDb.Collections;
using ENode.EventStore.MongoDb.Models;
using ENode.Infrastructure;
using MongoDB.Driver;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.EventStore.MongoDb
{
    public class MongoDbPublishedVersionStore : IPublishedVersionStore
    {
        #region Private Variables

        private readonly IOHelper _ioHelper;
        private readonly ILogger _logger;
        private readonly IPublishedVersionCollection _publishedVersionCollection;
        private readonly ISavableAggregateSnapshotter _savableAggregateSnapshotter;
        private readonly ITypeNameProvider _typeNameProvider;

        #endregion Private Variables

        #region Ctor

        public MongoDbPublishedVersionStore(
            IOHelper ioHelper,
            ILoggerFactory loggerFactory,
            IPublishedVersionCollection publishedVersionCollection,
            ISavableAggregateSnapshotter savableAggregateSnapshotter,
            ITypeNameProvider typeNameProvider
            )
        {
            _ioHelper = ioHelper;
            _logger = loggerFactory.Create(GetType().FullName);
            _publishedVersionCollection = publishedVersionCollection;
            _savableAggregateSnapshotter = savableAggregateSnapshotter;
            _typeNameProvider = typeNameProvider;
        }

        #endregion Ctor

        #region Public Methods

        public async Task<AsyncTaskResult<int>> GetPublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId)
        {
            try
            {
                var builder = Builders<PublishedVersion>.Filter;
                var filter = builder.Eq(e => e.ProcessorName, processorName) & builder.Eq(e => e.AggregateRootId, aggregateRootId);
                var result = await _publishedVersionCollection.GetCollection(aggregateRootId).Find(filter).ToListAsync();
                var version = result.Select(r => r.Version).SingleOrDefault();

                return new AsyncTaskResult<int>(AsyncTaskStatus.Success, version);
            }
            catch (MongoQueryException ex)
            {
                _logger.Error("Get aggregate published version has query exception.", ex);
                return new AsyncTaskResult<int>(AsyncTaskStatus.IOException, ex.Message);
            }
            catch (Exception ex)
            {
                _logger.Error("Get aggregate published version has unknown exception.", ex);
                return new AsyncTaskResult<int>(AsyncTaskStatus.Failed, ex.Message);
            }
        }

        public async Task<AsyncTaskResult> UpdatePublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId, int publishedVersion)
        {
            if (publishedVersion == 1)
            {
                var record = new PublishedVersion()
                {
                    ProcessorName = processorName,
                    AggregateRootTypeName = aggregateRootTypeName,
                    AggregateRootId = aggregateRootId,
                    Version = 1,
                    CreatedOn = DateTime.UtcNow
                };
                try
                {
                    await _publishedVersionCollection.GetCollection(aggregateRootId).InsertOneAsync(record);

                    return AsyncTaskResult.Success;
                }
                catch (MongoWriteException ex)
                {
                    if (ex.WriteError.Code == 11000 && ex.Message.Contains(nameof(record.ProcessorName)) && ex.Message.Contains(nameof(record.AggregateRootId)) && ex.Message.Contains(nameof(record.Version)))
                    {
                        return AsyncTaskResult.Success;
                    }
                    _logger.Error("Insert aggregate published version has write exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.IOException, ex.Message);
                }
                catch (Exception ex)
                {
                    _logger.Error("Insert aggregate published version has unknown exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.Failed, ex.Message);
                }
            }
            else
            {
                try
                {
                    var builder = Builders<PublishedVersion>.Filter;
                    var filter = builder.Eq(e => e.ProcessorName, processorName)
                        & builder.Eq(e => e.AggregateRootId, aggregateRootId)
                        & builder.Eq(e => e.Version, publishedVersion - 1);
                    var update = Builders<PublishedVersion>.Update
                        .Set(e => e.Version, publishedVersion)
                        .Set(e => e.CreatedOn, DateTime.UtcNow);

                    await _publishedVersionCollection.GetCollection(aggregateRootId)
                        .UpdateOneAsync(filter, update);

                    await _savableAggregateSnapshotter.SaveSnapshotAsync(aggregateRootId, _typeNameProvider.GetType(aggregateRootTypeName));

                    return AsyncTaskResult.Success;
                }
                catch (MongoException ex)
                {
                    _logger.Error("Update aggregate published version has update exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.IOException, ex.Message);
                }
                catch (Exception ex)
                {
                    _logger.Error("Update aggregate published version has unknown exception.", ex);
                    return new AsyncTaskResult(AsyncTaskStatus.Failed, ex.Message);
                }
            }
        }

        #endregion Public Methods
    }
}