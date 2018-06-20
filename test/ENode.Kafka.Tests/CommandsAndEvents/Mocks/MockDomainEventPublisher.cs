﻿using System;
using System.Threading.Tasks;
using ECommon.IO;
using ENode.Eventing;
using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Mocks
{
    public class MockDomainEventPublisher : IMessagePublisher<DomainEventStreamMessage>
    {
        private static Task<AsyncTaskResult> _successResultTask = Task.FromResult(AsyncTaskResult.Success);
        private int _currentFailedCount = 0;
        private int _expectFailedCount = 0;
        private FailedType _failedType;

        public Task<AsyncTaskResult> PublishAsync(DomainEventStreamMessage message)
        {
            if (_currentFailedCount < _expectFailedCount)
            {
                _currentFailedCount++;

                if (_failedType == FailedType.UnKnownException)
                {
                    throw new Exception("PublishDomainEventStreamMessageAsyncUnKnownException" + _currentFailedCount);
                }
                else if (_failedType == FailedType.IOException)
                {
                    throw new IOException("PublishDomainEventStreamMessageAsyncIOException" + _currentFailedCount);
                }
                else if (_failedType == FailedType.TaskIOException)
                {
                    return Task.FromResult(new AsyncTaskResult(AsyncTaskStatus.Failed, "PublishDomainEventStreamMessageAsyncError" + _currentFailedCount));
                }
            }
            return _successResultTask;
        }

        public void Reset()
        {
            _failedType = FailedType.None;
            _expectFailedCount = 0;
            _currentFailedCount = 0;
        }

        public void SetExpectFailedCount(FailedType failedType, int count)
        {
            _failedType = failedType;
            _expectFailedCount = count;
        }
    }
}