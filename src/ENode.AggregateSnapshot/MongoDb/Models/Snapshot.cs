using MongoDB.Bson;
using System;

namespace ENode.AggregateSnapshot.Models
{
    public class Snapshot
    {
        public string AggregateRootId { get; set; }

        public string AggregateRootTypeName { get; set; }

        public DateTime CreationTime { get; set; }

        public ObjectId Id { get; set; }

        public DateTime ModificationTime { get; set; }

        public string Payload { get; set; }

        public int Version { get; set; }
    }
}