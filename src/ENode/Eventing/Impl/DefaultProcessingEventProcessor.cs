﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using ECommon.IO;
using ECommon.Logging;
using ECommon.Scheduling;
using ENode.Configurations;
using ENode.Infrastructure;
using ENode.Messaging;

namespace ENode.Eventing.Impl
{
    public class DefaultProcessingEventProcessor : IProcessingEventProcessor
    {
        private readonly AsyncLock _asyncLock = new AsyncLock();
        private readonly IMessageDispatcher _dispatcher;
        private readonly IOHelper _ioHelper;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, ProcessingEventMailBox> _mailboxDict;
        private readonly string _processorName;
        private readonly IPublishedVersionStore _publishedVersionStore;
        private readonly int _scanExpiredAggregateIntervalMilliseconds;
        private readonly IScheduleService _scheduleService;
        private readonly string _taskName;
        private readonly int _timeoutSeconds;

        public DefaultProcessingEventProcessor(IPublishedVersionStore publishedVersionStore, IMessageDispatcher dispatcher, IOHelper ioHelper, ILoggerFactory loggerFactory, IScheduleService scheduleService)
        {
            _mailboxDict = new ConcurrentDictionary<string, ProcessingEventMailBox>();
            _publishedVersionStore = publishedVersionStore;
            _dispatcher = dispatcher;
            _ioHelper = ioHelper;
            _logger = loggerFactory.Create(GetType().FullName);
            _scheduleService = scheduleService;
            _taskName = "CleanInactiveProcessingEventMailBoxes_" + DateTime.Now.Ticks + new Random().Next(10000);
            _timeoutSeconds = ENodeConfiguration.Instance.Setting.AggregateRootMaxInactiveSeconds;
            _processorName = ENodeConfiguration.Instance.Setting.DomainEventProcessorName;
            _scanExpiredAggregateIntervalMilliseconds = ENodeConfiguration.Instance.Setting.ScanExpiredAggregateIntervalMilliseconds;
        }

        public async Task ProcessAsync(ProcessingEvent processingMessage)
        {
            var aggregateRootId = processingMessage.Message.AggregateRootId;
            if (string.IsNullOrEmpty(aggregateRootId))
            {
                throw new ArgumentException("aggregateRootId of domain event stream cannot be null or empty, domainEventStreamId:" + processingMessage.Message.Id);
            }

            using (await _asyncLock.LockAsync().ConfigureAwait(false))
            {
                if (!_mailboxDict.TryGetValue(aggregateRootId, out ProcessingEventMailBox mailbox))
                {
                    var latestHandledEventVersion = await GetAggregateRootLatestHandledEventVersionAsync(processingMessage.Message.AggregateRootTypeName, aggregateRootId);
                    mailbox = new ProcessingEventMailBox(aggregateRootId, latestHandledEventVersion, y => DispatchProcessingMessageAsync(y, 0), _logger);
                    _mailboxDict.TryAdd(aggregateRootId, mailbox);
                }

                if (!mailbox.EnqueueMessage(processingMessage))
                {
                    processingMessage.ProcessContext.NotifyEventProcessed();
                }
            }
        }

        public void Start()
        {
            _scheduleService.StartTask(_taskName, async () => { await CleanInactiveMailboxAsync(); }, _scanExpiredAggregateIntervalMilliseconds, _scanExpiredAggregateIntervalMilliseconds);
        }

        public void Stop()
        {
            _scheduleService.StopTask(_taskName);
        }

        private async Task CleanInactiveMailboxAsync()
        {
            var inactiveList = new List<KeyValuePair<string, ProcessingEventMailBox>>();
            foreach (var pair in _mailboxDict)
            {
                if (pair.Value.IsInactive(_timeoutSeconds) && !pair.Value.IsRunning)
                {
                    inactiveList.Add(pair);
                }
            }
            foreach (var pair in inactiveList)
            {
                using (await _asyncLock.LockAsync().ConfigureAwait(false))
                {
                    if (pair.Value.IsInactive(_timeoutSeconds) && !pair.Value.IsRunning && pair.Value.TotalUnHandledMessageCount == 0)
                    {
                        if (_mailboxDict.TryRemove(pair.Key, out ProcessingEventMailBox removed))
                        {
                            _logger.InfoFormat("Removed inactive domain event stream mailbox, aggregateRootId: {0}", pair.Key);
                        }
                    }
                }
            }
        }

        private void DispatchProcessingMessageAsync(ProcessingEvent processingMessage, int retryTimes)
        {
            _ioHelper.TryAsyncActionRecursivelyWithoutResult("DispatchProcessingMessageAsync",
            () => _dispatcher.DispatchMessagesAsync(processingMessage.Message.Events),
            currentRetryTimes => DispatchProcessingMessageAsync(processingMessage, currentRetryTimes),
            () =>
            {
                UpdatePublishedVersionAsync(processingMessage, 0);
            },
            () => string.Format("Message[messageId:{0}, messageType:{1}, aggregateRootId:{2}, aggregateRootVersion:{3}]", processingMessage.Message.Id, processingMessage.Message.GetType().Name, processingMessage.Message.AggregateRootId, processingMessage.Message.Version),
            null,
            retryTimes, true);
        }

        private Task<int> GetAggregateRootLatestHandledEventVersionAsync(string aggregateRootType, string aggregateRootId)
        {
            try
            {
                return _publishedVersionStore.GetPublishedVersionAsync(_processorName, aggregateRootType, aggregateRootId);
            }
            catch (Exception ex)
            {
                throw new Exception(string.Format("_publishedVersionStore.GetPublishedVersionAsync has unknown exception, aggregateRootType: {0}, aggregateRootId: {1}", aggregateRootType, aggregateRootId), ex);
            }
        }

        private void UpdatePublishedVersionAsync(ProcessingEvent processingMessage, int retryTimes)
        {
            var message = processingMessage.Message;
            _ioHelper.TryAsyncActionRecursivelyWithoutResult("UpdatePublishedVersionAsync",
            () => _publishedVersionStore.UpdatePublishedVersionAsync(_processorName, message.AggregateRootTypeName, message.AggregateRootId, message.Version),
            currentRetryTimes => UpdatePublishedVersionAsync(processingMessage, currentRetryTimes),
            () =>
            {
                processingMessage.Complete();
            },
            () => string.Format("DomainEventStreamMessage [messageId:{0}, messageType:{1}, aggregateRootId:{2}, aggregateRootVersion:{3}]", message.Id, message.GetType().Name, message.AggregateRootId, message.Version),
            null,
            retryTimes, true);
        }
    }
}