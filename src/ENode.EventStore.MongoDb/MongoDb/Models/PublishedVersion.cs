using MongoDB.Bson;
using System;

namespace ENode.EventStore.MongoDb.Models
{
    public class PublishedVersion
    {
        public string AggregateRootId { get; set; }

        public string AggregateRootTypeName { get; set; }

        public DateTime CreatedOn { get; set; }

        public ObjectId Id { get; set; }

        public string ProcessorName { get; set; }

        public int Version { get; set; }
    }
}