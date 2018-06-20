﻿using System;
using System.Threading.Tasks;
using ECommon.IO;
using ENode.Infrastructure;
using ENode.Infrastructure.Impl.InMemory;

namespace ENode.Kafka.Tests.CommandsAndEvents.Mocks
{
    public class MockPublishedVersionStore : IPublishedVersionStore
    {
        private int _currentGetFailedCount = 0;
        private int _currentUpdateFailedCount = 0;
        private int _expectGetFailedCount = 0;
        private int _expectUpdateFailedCount = 0;
        private FailedType _failedType;
        private InMemoryPublishedVersionStore _inMemoryPublishedVersionStore = new InMemoryPublishedVersionStore();

        public Task<AsyncTaskResult<int>> GetPublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId)
        {
            if (_currentGetFailedCount < _expectGetFailedCount)
            {
                _currentGetFailedCount++;

                if (_failedType == FailedType.UnKnownException)
                {
                    throw new Exception("GetPublishedVersionAsyncUnKnownException" + _currentGetFailedCount);
                }
                else if (_failedType == FailedType.IOException)
                {
                    throw new IOException("GetPublishedVersionAsyncIOException" + _currentGetFailedCount);
                }
                else if (_failedType == FailedType.TaskIOException)
                {
                    return Task.FromResult(new AsyncTaskResult<int>(AsyncTaskStatus.Failed, "GetPublishedVersionAsyncError" + _currentGetFailedCount));
                }
            }
            return _inMemoryPublishedVersionStore.GetPublishedVersionAsync(processorName, aggregateRootTypeName, aggregateRootId);
        }

        public void Reset()
        {
            _failedType = FailedType.None;
            _expectGetFailedCount = 0;
            _expectUpdateFailedCount = 0;
            _currentGetFailedCount = 0;
            _currentUpdateFailedCount = 0;
        }

        public void SetExpectFailedCount(FailedType failedType, int count)
        {
            _failedType = failedType;
            _expectGetFailedCount = count;
            _expectUpdateFailedCount = count;
        }

        public Task<AsyncTaskResult> UpdatePublishedVersionAsync(string processorName, string aggregateRootTypeName, string aggregateRootId, int publishedVersion)
        {
            if (_currentUpdateFailedCount < _expectUpdateFailedCount)
            {
                _currentUpdateFailedCount++;

                if (_failedType == FailedType.UnKnownException)
                {
                    throw new Exception("UpdatePublishedVersionAsyncUnKnownException" + _currentUpdateFailedCount);
                }
                else if (_failedType == FailedType.IOException)
                {
                    throw new IOException("UpdatePublishedVersionAsyncIOException" + _currentUpdateFailedCount);
                }
                else if (_failedType == FailedType.TaskIOException)
                {
                    return Task.FromResult(new AsyncTaskResult(AsyncTaskStatus.Failed, "UpdatePublishedVersionAsyncError" + _currentUpdateFailedCount));
                }
            }
            return _inMemoryPublishedVersionStore.UpdatePublishedVersionAsync(processorName, aggregateRootTypeName, aggregateRootId, publishedVersion);
        }
    }
}