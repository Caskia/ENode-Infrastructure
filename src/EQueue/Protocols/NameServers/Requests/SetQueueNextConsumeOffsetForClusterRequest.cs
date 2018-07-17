﻿using System;

namespace EQueue.Protocols.NameServers.Requests
{
    [Serializable]
    public class SetQueueNextConsumeOffsetForClusterRequest
    {
        public string ClusterName { get; set; }
        public string ConsumerGroup { get; set; }
        public string Topic { get; set; }
        public int QueueId { get; set; }
        public long NextOffset { get; set; }
    }
}
