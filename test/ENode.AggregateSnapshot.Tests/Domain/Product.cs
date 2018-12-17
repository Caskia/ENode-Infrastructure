using ENode.Domain;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ENode.AggregateSnapshot.Tests.Domain
{
    public class Product : AggregateRoot<long>
    {
        #region Fields

        private bool _isPublished;

        private string _name;
        private IDictionary<long, ProductRecord> _records;
        private ISet<long> _userIds;

        #endregion Fields

        #region Properties

        public bool IsPublished
        {
            get
            {
                return _isPublished;
            }
        }

        public string Name
        {
            get
            {
                return _name;
            }
        }

        #endregion Properties

        public Product()
        {
        }

        public Product(long id, string name, bool isPublished, List<long> userIds, List<ProductRecord> records) : base(id)
        {
            _id = id;
            _name = name;
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