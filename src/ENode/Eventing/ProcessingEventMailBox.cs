﻿using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Logging;

namespace ENode.Eventing
{
    public class ProcessingEventMailBox
    {
        public enum EnqueueMessageResult
        {
            Success,
            AddToWaitingList,
            Ignored
        }
        #region Private Variables 

        private int _nextExpectingEventVersion;
        private volatile int _isUsing;
        private volatile int _isRemoved;
        private volatile int _isRunning;
        private readonly object _lockObj = new object();
        private readonly ConcurrentQueue<ProcessingEvent> _processingEventQueue;
        private readonly ConcurrentDictionary<int, ProcessingEvent> _waitingProcessingEventDict = new ConcurrentDictionary<int, ProcessingEvent>();
        private readonly Action<ProcessingEvent> _handleProcessingEventAction;
        private readonly ILogger _logger;

        #endregion

        public ProcessingEventMailBox(string aggregateRootTypeName, string aggregateRootId, int nextExpectingEventVersion, Action<ProcessingEvent> handleProcessingEventAction, ILogger logger)
        {
            _processingEventQueue = new ConcurrentQueue<ProcessingEvent>();
            _handleProcessingEventAction = handleProcessingEventAction;
            _logger = logger;
            AggregateRootId = aggregateRootId;
            AggregateRootTypeName = aggregateRootTypeName;
            _nextExpectingEventVersion = nextExpectingEventVersion;
            LastActiveTime = DateTime.Now;
        }

        public string AggregateRootId { get; private set; }
        public string AggregateRootTypeName { get; private set; }
        public bool IsUsing { get { return _isUsing == 1; } }
        public bool IsRunning { get { return _isRunning == 1; } }
        public bool IsRemoved { get { return _isRemoved == 1; } }
        public long TotalUnHandledMessageCount
        {
            get
            {
                return _processingEventQueue.Count;
            }
        }
        public long WaitingMessageCount
        {
            get { return _waitingProcessingEventDict.Count; }
        }
        public DateTime LastActiveTime { get; private set; }

        public void SetNextExpectingEventVersion(int nextExpectingEventVersion)
        {
            lock (_lockObj)
            {
                if (nextExpectingEventVersion > _nextExpectingEventVersion)
                {
                    _nextExpectingEventVersion = nextExpectingEventVersion;
                    _logger.InfoFormat("{0} refreshed next expecting event version, aggregateRootId: {1}, aggregateRootTypeName: {2}", GetType().Name, AggregateRootId, AggregateRootTypeName);
                    TryEnqueueWaitingMessage();
                }
            }
        }
        public EnqueueMessageResult EnqueueMessage(ProcessingEvent processingEvent)
        {
            lock (_lockObj)
            {
                if (processingEvent.Message.Version == _nextExpectingEventVersion)
                {
                    EnqueueEventStream(processingEvent);
                    TryEnqueueWaitingMessage();
                    LastActiveTime = DateTime.Now;
                    TryRun();
                    return EnqueueMessageResult.Success;
                }
                else if (processingEvent.Message.Version > _nextExpectingEventVersion)
                {
                    if (_waitingProcessingEventDict.TryAdd(processingEvent.Message.Version, processingEvent))
                    {
                        _logger.WarnFormat("{0} later version of message arrived, added it to the waiting list, aggregateRootType: {1}, aggregateRootId: {2}, commandId: {3}, eventVersion: {4}, eventStreamId: {5}, eventTypes: {6}, eventIds: {7}, _nextExpectingEventVersion: {8}",
                            GetType().Name,
                            processingEvent.Message.AggregateRootTypeName,
                            processingEvent.Message.AggregateRootId,
                            processingEvent.Message.CommandId,
                            processingEvent.Message.Version,
                            processingEvent.Message.Id,
                            string.Join("|", processingEvent.Message.Events.Select(x => x.GetType().Name)),
                            string.Join("|", processingEvent.Message.Events.Select(x => x.Id)),
                            _nextExpectingEventVersion
                        );
                    }
                    return EnqueueMessageResult.AddToWaitingList;
                }
                return EnqueueMessageResult.Ignored;
            }
        }
        public void TryRun()
        {
            lock (_lockObj)
            {
                if (IsRunning)
                {
                    return;
                }
                SetAsRunning();
                _logger.DebugFormat("{0} start run, aggregateRootId: {1}", GetType().Name, AggregateRootId);
                Task.Factory.StartNew(ProcessMessage);
            }
        }
        public void CompleteRun()
        {
            LastActiveTime = DateTime.Now;
            _logger.DebugFormat("{0} complete run, aggregateRootId: {1}", GetType().Name, AggregateRootId);
            SetAsNotRunning();
            if (TotalUnHandledMessageCount > 0)
            {
                TryRun();
            }
        }
        public bool IsInactive(int timeoutSeconds)
        {
            return (DateTime.Now - LastActiveTime).TotalSeconds >= timeoutSeconds;
        }
        public bool TryUsing()
        {
            return Interlocked.CompareExchange(ref _isUsing, 1, 0) == 0;
        }
        public void ExitUsing()
        {
            Interlocked.Exchange(ref _isUsing, 0);
        }
        public void MarkAsRemoved()
        {
            Interlocked.Exchange(ref _isRemoved, 1);
        }

        private void TryEnqueueWaitingMessage()
        {
            while (_waitingProcessingEventDict.TryRemove(_nextExpectingEventVersion, out ProcessingEvent nextProcessingEvent))
            {
                EnqueueEventStream(nextProcessingEvent);
            }
        }
        private void ProcessMessage()
        {
            if (_processingEventQueue.TryDequeue(out ProcessingEvent message))
            {
                LastActiveTime = DateTime.Now;
                try
                {
                    _handleProcessingEventAction(message);
                }
                catch (Exception ex)
                {
                    _logger.Error(string.Format("{0} run has unknown exception, aggregateRootId: {1}, aggregateRootTypeName: {2}", GetType().Name, AggregateRootId, AggregateRootTypeName), ex);
                    Thread.Sleep(1);
                    CompleteRun();
                }
            }
            else
            {
                CompleteRun();
            }
        }
        private void SetAsRunning()
        {
            Interlocked.Exchange(ref _isRunning, 1);
        }
        private void SetAsNotRunning()
        {
            Interlocked.Exchange(ref _isRunning, 0);
        }
        private void EnqueueEventStream(ProcessingEvent processingEvent)
        {
            lock (_lockObj)
            {
                processingEvent.MailBox = this;
                _processingEventQueue.Enqueue(processingEvent);
                _nextExpectingEventVersion = processingEvent.Message.Version + 1;
                if (_logger.IsDebugEnabled)
                {
                    _logger.DebugFormat("{0} enqueued new message, aggregateRootType: {1}, aggregateRootId: {2}, commandId: {3}, eventVersion: {4}, eventStreamId: {5}, eventTypes: {6}, eventIds: {7}",
                        GetType().Name,
                        processingEvent.Message.AggregateRootTypeName,
                        processingEvent.Message.AggregateRootId,
                        processingEvent.Message.CommandId,
                        processingEvent.Message.Version,
                        processingEvent.Message.Id,
                        string.Join("|", processingEvent.Message.Events.Select(x => x.GetType().Name)),
                        string.Join("|", processingEvent.Message.Events.Select(x => x.Id))
                    );
                }
            }
        }
    }
}