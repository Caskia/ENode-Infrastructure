﻿using System;
using System.Threading.Tasks;
using ECommon.IO;
using ENode.Infrastructure;

namespace ENode.Kafka.Tests.CommandsAndEvents.Mocks
{
    public class MockPublishableExceptionPublisher : IMessagePublisher<IPublishableException>
    {
        private static Task<AsyncTaskResult> _successResultTask = Task.FromResult(AsyncTaskResult.Success);
        private int _currentFailedCount = 0;
        private int _expectFailedCount = 0;
        private FailedType _failedType;

        public Task<AsyncTaskResult> PublishAsync(IPublishableException message)
        {
            if (_currentFailedCount < _expectFailedCount)
            {
                _currentFailedCount++;

                if (_failedType == FailedType.UnKnownException)
                {
                    throw new Exception("PublishPublishableExceptionAsyncUnKnownException" + _currentFailedCount);
                }
                else if (_failedType == FailedType.IOException)
                {
                    throw new IOException("PublishPublishableExceptionAsyncIOException" + _currentFailedCount);
                }
                else if (_failedType == FailedType.TaskIOException)
                {
                    return Task.FromResult(new AsyncTaskResult(AsyncTaskStatus.Failed, "PublishPublishableExceptionAsyncError" + _currentFailedCount));
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