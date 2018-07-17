﻿using System;

namespace EQueue.Protocols.Brokers.Requests
{
    [Serializable]
    public class SetQueueProducerVisibleRequest
    {
        public string Topic { get; private set; }
        public int QueueId { get; private set; }
        public bool Visible { get; private set; }

        public SetQueueProducerVisibleRequest(string topic, int queueId, bool visible)
        {
            Topic = topic;
            QueueId = queueId;
            Visible = visible;
        }
    }
}
