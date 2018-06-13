using Confluent.Kafka;
using ECommon.Extensions;
using System.Collections.Generic;
using System.Linq;

namespace ENode.Kafka.Consumers
{
    public class ProcessQueue<TKey, TValue>
    {
        private readonly object _lockObj = new object();
        private readonly SortedDictionary<long, Message<TKey, TValue>> _messageDict = new SortedDictionary<long, Message<TKey, TValue>>();
        private long _consumedQueueOffset = -1L;
        private long _maxQueueOffset = -1L;
        private int _messageCount = 0;
        private long _previousConsumedQueueOffset = -1L;

        public void AddMessage(Message<TKey, TValue> consumingMessage)
        {
            lock (_lockObj)
            {
                AddMessageWithoutLock(consumingMessage);
            }
        }

        public void AddMessages(IEnumerable<Message<TKey, TValue>> consumingMessages)
        {
            lock (_lockObj)
            {
                foreach (var consumingMessage in consumingMessages)
                {
                    AddMessageWithoutLock(consumingMessage);
                }
            }
        }

        public long GetConsumedQueueOffset()
        {
            return _consumedQueueOffset;
        }

        public int GetMessageCount()
        {
            return _messageCount;
        }

        public void RemoveMessage(Message<TKey, TValue> consumingMessage)
        {
            lock (_lockObj)
            {
                if (_messageDict.Remove(consumingMessage.Offset.Value))
                {
                    if (_messageDict.Keys.IsNotEmpty())
                    {
                        _consumedQueueOffset = _messageDict.Keys.First() - 1;
                    }
                    else
                    {
                        _consumedQueueOffset = _maxQueueOffset;
                    }
                    _messageCount--;
                }
            }
        }

        public void Reset()
        {
            lock (_lockObj)
            {
                _messageDict.Clear();
                _consumedQueueOffset = -1L;
                _previousConsumedQueueOffset = -1L;
                _maxQueueOffset = -1;
                _messageCount = 0;
            }
        }

        public bool TryUpdatePreviousConsumedQueueOffset(long current)
        {
            if (current != _previousConsumedQueueOffset)
            {
                _previousConsumedQueueOffset = current;
                return true;
            }
            return false;
        }

        private void AddMessageWithoutLock(Message<TKey, TValue> consumingMessage)
        {
            if (_messageDict.ContainsKey(consumingMessage.Offset.Value))
            {
                return;
            }
            _messageDict[consumingMessage.Offset.Value] = consumingMessage;
            if (_maxQueueOffset == -1 && consumingMessage.Offset.Value >= 0)
            {
                _maxQueueOffset = consumingMessage.Offset.Value;
            }
            else if (consumingMessage.Offset.Value > _maxQueueOffset)
            {
                _maxQueueOffset = consumingMessage.Offset.Value;
            }
            _messageCount++;
        }
    }
}