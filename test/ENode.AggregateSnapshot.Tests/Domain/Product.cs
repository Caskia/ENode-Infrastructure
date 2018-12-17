using ENode.Domain;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ENode.AggregateSnapshot.Tests.Domain
{
    public class Product : AggregateRoot<long>
    {
        private bool _isPublished;

        private IDictionary<long, ProductRecord> _records;
        private ISet<long> _userIds;

        public Product(long id, bool isPublished, List<long> userIds, List<ProductRecord> records) : base(id)
        {
            _id = id;
            _isPublished = isPublished;
            _userIds = new HashSet<long>(userIds);
            _records = records.ToDictionary(r => r.Id, r => r);
        }
    }

    [Serializable]
    public class ProductRecord
    {
        public ProductRecord()
        {
        }

        public ProductRecord(long id, string name, DateTime time)
        {
            Id = id;
            Name = name;
            Time = time;
        }

        public long Id { get; set; }

        public string Name { get; private set; }

        public DateTime Time { get; private set; }
    }
}