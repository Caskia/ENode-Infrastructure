using ECommon.IO;
using ECommon.Logging;
using ECommon.Serializing;
using ENode.Eventing;
using ENode.EventStore.MongoDb.Collections;
using ENode.EventStore.MongoDb.Models;
using MongoDB.Driver;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ENode.EventStore.MongoDb
{
    public class MongoDbEventStore : IEventStore
    {
        #region Private Variables

        private readonly IEventSerializer _eventSerializer;
        private readonly IEventStreamCollection _eventStreamCollection;
        private readonly IOHelper _ioHelper;
        private readonly IJsonSerializer _jsonSerializer;
        private readonly ILogger _logger;

        #endregion Private Variables

        #region Public Variables

        public bool SupportBatchAppendEvent { get; set; }

        #endregion Public Variables

        #region Ctor

        public MongoDbEventStore(
            IEventSerializer eventSerializer,
            IEventStreamCollection eventStreamCollection,
            IOHelper ioHelper,
            IJsonSerializer jsonSerializer,
            ILoggerFactory loggerFactory
            )
        {
            SupportBatchAppendEvent = false;

            _eventSerializer = eventSerializer;
            _eventStreamCollection = eventStreamCollection;
            _ioHelper = ioHelper;
            _jsonSerializer = jsonSerializer;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        #endregion Ctor

        #region Public Methods

        public Task<EventAppendResult> BatchAppendAsync(IEnumerable<DomainEventStream> eventStreams)
        {
            if (eventStreams.Count() == 0)
            {
                return Task.FromResult(new EventAppendResult());
            }

            var eventStreamDict = new Dictionary<string, IList<DomainEventStream>>();
            var aggregateRootIdList = eventStreams.Select(x => x.AggregateRootId).Distinct().ToList();
            foreach (var aggregateRootId in aggregateRootIdList)
            {
                var eventStreamList = eventStreams.Where(x => x.AggregateRootId == aggregateRootId).ToList();
                if (eventStreamList.Count > 0)
                {
                    eventStreamDict.Add(aggregateRootId, eventStreamList);
                }
            }

            var batchAggregateEventAppendResult = new BatchAggregateEventAppendResult(eventStreamDict.Keys.Count);
            foreach (var entry in eventStreamDict)
            {
                BatchAppendAggregateEventsAsync(entry.Key, entry.Value, batchAggregateEventAppendResult, 0);
            }

            return batchAggregateEventAppendResult.TaskCompletionSource.Task;
        }

        public Task<DomainEventStream> FindAsync(string aggregateRootId, int version)
        {
            return _ioHelper.TryIOFuncAsync(async () =>
            {
                try
                {
                    var builder = Builders<EventStream>.Filter;
                    var filter = builder.Eq(e => e.AggregateRootId, aggregateRootId) & builder.Eq(e => e.Version, version);
                    var result = await _eventStreamCollection.GetCollection(aggregateRootId).FindAsync(filter);
                    var record = result.SingleOrDefault();
                    return record != null ? ConvertFrom(record) : null;
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Find event by version has unknown exception, aggregateRootId: {0}, version: {1}", aggregateRootId, version), ex);
                    throw;
                }
            }, "FindEventByVersionAsync");
        }

        public Task<DomainEventStream> FindAsync(string aggregateRootId, string commandId)
        {
            return _ioHelper.TryIOFuncAsync(async () =>
            {
                try
                {
                    var builder = Builders<EventStream>.Filter;
                    var filter = builder.Eq(e => e.AggregateRootId, aggregateRootId) & builder.Eq(e => e.CommandId, commandId);
                    var result = await _eventStreamCollection.GetCollection(aggregateRootId).FindAsync(filter);
                    var record = result.SingleOrDefault();
                    return record != null ? ConvertFrom(record) : null;
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("Find event by commandId has unknown exception, aggregateRootId: {0}, commandId: {1}", aggregateRootId, commandId), ex);
                    throw;
                }
            }, "FindEventByCommandIdAsync");
        }

        public Task<IEnumerable<DomainEventStream>> QueryAggregateEventsAsync(string aggregateRootId, string aggregateRootTypeName, int minVersion, int maxVersion)
        {
            return _ioHelper.TryIOFuncAsync(async () =>
            {
                try
                {
                    var builder = Builders<EventStream>.Filter;

                    var filter = builder.Eq(e => e.AggregateRootId, aggregateRootId)
                    & builder.Gte(e => e.Version, minVersion)
                    & builder.Lte(e => e.Version, maxVersion);

                    var sort = Builders<EventStream>.Sort.Ascending(e => e.Version);

                    var result = await _eventStreamCollection.GetCollection(aggregateRootId)
                    .Find(filter)
                    .Sort(sort)
                    .ToListAsync();

                    return result.Select(ConvertFrom);
                }
                catch (Exception ex)
                {
                    var errorMessage = string.Format("Failed to query aggregate events async, aggregateRootId: {0}, aggregateRootType: {1}", aggregateRootId, aggregateRootTypeName);
                    _logger.Error(errorMessage, ex);
                    throw;
                }
            }, "QueryAggregateEventsAsync");
        }

        #endregion Public Methods

        #region Private Methods

        private void BatchAppendAggregateEventsAsync(string aggregateRootId, IList<DomainEventStream> eventStreamList, BatchAggregateEventAppendResult batchAggregateEventAppendResult, int retryTimes)
        {
            _ioHelper.TryAsyncActionRecursively("BatchAppendAggregateEventsAsync",
            () => BatchAppendAggregateEventsAsync(aggregateRootId, eventStreamList),
            currentRetryTimes => BatchAppendAggregateEventsAsync(aggregateRootId, eventStreamList, batchAggregateEventAppendResult, currentRetryTimes),
            async result =>
            {
                if (result == EventAppendStatus.Success)
                {
                    batchAggregateEventAppendResult.AddCompleteAggregate(aggregateRootId, new AggregateEventAppendResult
                    {
                        EventAppendStatus = EventAppendStatus.Success
                    });
                }
                else if (result == EventAppendStatus.DuplicateEvent)
                {
                    batchAggregateEventAppendResult.AddCompleteAggregate(aggregateRootId, new AggregateEventAppendResult
                    {
                        EventAppendStatus = EventAppendStatus.DuplicateEvent
                    });
                }
                else if (result == EventAppendStatus.DuplicateCommand)
                {
                    var duplicateCommandIds = new List<string>();
                    foreach (var eventStream in eventStreamList)
                    {
                        await TryFindEventByCommandIdAsync(aggregateRootId, eventStream.CommandId, duplicateCommandIds, 0);
                    }
                    batchAggregateEventAppendResult.AddCompleteAggregate(aggregateRootId, new AggregateEventAppendResult
                    {
                        EventAppendStatus = EventAppendStatus.DuplicateCommand,
                        DuplicateCommandIds = duplicateCommandIds
                    });
                }
            },
            () => string.Format("[aggregateRootId: {0}, eventStreamCount: {1}]", aggregateRootId, eventStreamList.Count),
            null,
            retryTimes, true);
        }

        private async Task<EventAppendStatus> BatchAppendAggregateEventsAsync(string aggregateRootId, IList<DomainEventStream> eventStreamList)
        {
            try
            {
                var collection = _eventStreamCollection.GetCollection(aggregateRootId);
                var streamRecords = eventStreamList.Select(ConvertTo);
                await collection.InsertManyAsync(streamRecords);

                return EventAppendStatus.Success;
            }
            catch (MongoBulkWriteException ex)
            {
                var duplicateWriteError = ex.WriteErrors.FirstOrDefault(w => w.Code == 11000);
                if (duplicateWriteError != null)
                {
                    if (duplicateWriteError.Message.Contains(nameof(EventStream.AggregateRootId)) && duplicateWriteError.Message.Contains(nameof(EventStream.Version)))
                    {
                        return EventAppendStatus.DuplicateEvent;
                    }

                    if (duplicateWriteError.Message.Contains(nameof(EventStream.AggregateRootId)) && duplicateWriteError.Message.Contains(nameof(EventStream.CommandId)))
                    {
                        return EventAppendStatus.DuplicateCommand;
                    }
                }

                throw;
            }
        }

        private DomainEventStream ConvertFrom(EventStream record)
        {
            return new DomainEventStream(
               record.CommandId,
               record.AggregateRootId,
               record.AggregateRootTypeName,
               record.CreatedOn,
               _eventSerializer.Deserialize<IDomainEvent>(_jsonSerializer.Deserialize<IDictionary<string, string>>(record.Events)));
        }

        private EventStream ConvertTo(DomainEventStream eventStream)
        {
            return new EventStream
            {
                CommandId = eventStream.CommandId,
                AggregateRootId = eventStream.AggregateRootId,
                AggregateRootTypeName = eventStream.AggregateRootTypeName,
                Version = eventStream.Version,
                CreatedOn = eventStream.Timestamp,
                Events = _jsonSerializer.Serialize(_eventSerializer.Serialize(eventStream.Events))
            };
        }

        private Task TryFindEventByCommandIdAsync(string aggregateRootId, string commandId, IList<string> duplicateCommandIds, int retryTimes)
        {
            var taskCompletionSource = new TaskCompletionSource<bool>();

            _ioHelper.TryAsyncActionRecursively("TryFindEventByCommandIdAsync",
            () => FindAsync(aggregateRootId, commandId),
            currentRetryTimes => TryFindEventByCommandIdAsync(aggregateRootId, commandId, duplicateCommandIds, currentRetryTimes),
            result =>
            {
                if (result != null)
                {
                    duplicateCommandIds.Add(commandId);
                }
                taskCompletionSource.SetResult(true);
            },
            () => string.Format("[aggregateRootId:{0}, commandId:{1}]", aggregateRootId, commandId),
            null,
            retryTimes, true);

            return taskCompletionSource.Task;
        }

        #endregion Private Methods

        private class AggregateEventAppendResult
        {
            public IList<string> DuplicateCommandIds;
            public EventAppendStatus EventAppendStatus;
        }

        private class BatchAggregateEventAppendResult
        {
            public TaskCompletionSource<EventAppendResult> TaskCompletionSource = new TaskCompletionSource<EventAppendResult>();
            private readonly int _expectedAggregateRootCount;
            private ConcurrentDictionary<string, AggregateEventAppendResult> _aggregateEventAppendResultDict = new ConcurrentDictionary<string, AggregateEventAppendResult>();

            public BatchAggregateEventAppendResult(int expectedAggregateRootCount)
            {
                _expectedAggregateRootCount = expectedAggregateRootCount;
            }

            public void AddCompleteAggregate(string aggregateRootId, AggregateEventAppendResult result)
            {
                if (_aggregateEventAppendResultDict.TryAdd(aggregateRootId, result))
                {
                    var completedAggregateRootCount = _aggregateEventAppendResultDict.Keys.Count;
                    if (completedAggregateRootCount == _expectedAggregateRootCount)
                    {
                        var eventAppendResult = new EventAppendResult();
                        foreach (var entry in _aggregateEventAppendResultDict)
                        {
                            if (entry.Value.EventAppendStatus == EventAppendStatus.Success)
                            {
                                eventAppendResult.AddSuccessAggregateRootId(entry.Key);
                            }
                            else if (entry.Value.EventAppendStatus == EventAppendStatus.DuplicateEvent)
                            {
                                eventAppendResult.AddDuplicateEventAggregateRootId(entry.Key);
                            }
                            else if (entry.Value.EventAppendStatus == EventAppendStatus.DuplicateCommand)
                            {
                                eventAppendResult.AddDuplicateCommandIds(entry.Key, entry.Value.DuplicateCommandIds);
                            }
                        }
                        TaskCompletionSource.TrySetResult(eventAppendResult);
                    }
                }
            }
        }
    }
}