﻿using System;

namespace EQueue.Protocols.NameServers
{
    [Serializable]
    public class TopicAccumulateInfo
    {
        public string Topic { get; set; }
        public int QueueCount { get; set; }
        public string ConsumerGroup { get; set; }
        public long AccumulateCount { get; set; }
        public int OnlineConsumerCount { get; set; }
        public long ConsumeThroughput { get; set; }
    }
}
