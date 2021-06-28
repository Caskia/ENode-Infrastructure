﻿using ECommon.IO;
using ENode.Eventing;
using ENode.Messaging;
using System;
using System.Threading.Tasks;

namespace ENode.Kafka.Tests.CommandsAndEvents.Mocks
{
    public class MockDomainEventPublisher : IMessagePublisher<DomainEventStreamMessage>
    {
        private int _currentFailedCount = 0;
        private int _expectFailedCount = 0;
        private FailedType _failedType;

        public Task PublishAsync(DomainEventStreamMessage message)
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
            }
            return Task.CompletedTask;
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