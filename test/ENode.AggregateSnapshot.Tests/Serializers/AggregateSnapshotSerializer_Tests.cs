using ECommon.Components;
using ENode.AggregateSnapshot.Serializers;
using ENode.AggregateSnapshot.Tests.Domain;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace ENode.AggregateSnapshot.Tests.Serializers
{
    public class AggregateSnapshotSerializer_Tests : TestBase
    {
        private readonly IAggregateSnapshotSerializer _serializer;

        public AggregateSnapshotSerializer_Tests()
        {
            _serializer = ObjectContainer.Resolve<IAggregateSnapshotSerializer>();
        }

        [Fact]
        public void JsonSerializer_Test()
        {
            var product = new Product
                (
                    1L,
                    true,
                    new List<long>()
                    {
                        1L,
                        2L,
                        3L
                    },
                    new List<ProductRecord>()
                    {
                        new ProductRecord(1L,"a",DateTime.Now),
                        new ProductRecord(2L,"b",DateTime.Now),
                        new ProductRecord(3L,"c",DateTime.Now),
                        new ProductRecord(4L,"d",DateTime.Now),
                    }
                );

            var json = _serializer.Serialize(product);
        }
    }
}