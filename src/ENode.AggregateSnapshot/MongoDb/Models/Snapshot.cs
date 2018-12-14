using MongoDB.Bson;

namespace ENode.AggregateSnapshot.Models
{
    public class Snapshot
    {
        public string AggregateRootId { get; set; }

        public string AggregateRootTypeName { get; set; }

        public ObjectId Id { get; set; }

        public string Payload { get; set; }
    }
}