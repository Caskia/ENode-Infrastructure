using System;
using System.Collections.Generic;

namespace ENode.Kafka
{
    [Serializable]
    public class PublishableExceptionMessage
    {
        public string AggregateRootId { get; set; }

        public string AggregateRootTypeName { get; set; }

        public IDictionary<string, string> SerializableInfo { get; set; }

        public DateTime Timestamp { get; set; }

        public string UniqueId { get; set; }
    }
}