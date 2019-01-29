using System;
using System.Text;

namespace ENode.AggregateSnapshot.Models
{
    public class Snapshot
    {
        public string AggregateRootId { get; set; }

        public string AggregateRootTypeName { get; set; }

        public DateTime CreationTime { get; set; }

        public DateTime? ModificationTime { get; set; }

        public byte[] Payload { get; set; }

        public string SerializedPayload
        {
            get
            {
                if (Payload != null && Payload.Length > 0)
                {
                    return Encoding.UTF8.GetString(Payload);
                }
                return null;
            }
            set
            {
                if (!string.IsNullOrEmpty(value))
                {
                    Payload = Encoding.UTF8.GetBytes(value);
                }
            }
        }

        public int Version { get; set; }
    }
}