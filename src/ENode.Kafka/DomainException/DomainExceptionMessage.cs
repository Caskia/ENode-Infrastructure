using System;
using System.Collections.Generic;

namespace ENode.Kafka
{
    [Serializable]
    public class DomainExceptionMessage
    {
        public IDictionary<string, string> Items { get; set; }

        public IDictionary<string, string> SerializableInfo { get; set; }

        public DateTime Timestamp { get; set; }

        public string UniqueId { get; set; }
    }
}