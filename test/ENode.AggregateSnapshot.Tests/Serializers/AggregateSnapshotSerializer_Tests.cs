using ECommon.Components;
using ENode.AggregateSnapshot.Serializers;
using ENode.AggregateSnapshot.Tests.Domain;
using ENode.Domain;
using Shouldly;
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
        public void JsonSerializerAndDeserializer_Test()
        {
            //Arrange
            var product = new Product
                (
                    1L,
                    "banana",
                    true,
                    new List<long>()
                    {
                        1L,
                        2L,
                        3L
                    },
                    new List<ProductRecord>()
                    {
                        new ProductRecord(1L,"a",DateTime.UtcNow),
                        new ProductRecord(2L,"b",DateTime.UtcNow),
                        new ProductRecord(3L,"c",DateTime.UtcNow),
                        new ProductRecord(4L,"d",DateTime.UtcNow),
                    }
                );

            //Act
            var json = _serializer.Serialize(product);

            var deserializedProduct = _serializer.Deserialize(json, typeof(Product)) as Product;

            //Assert
            deserializedProduct.Id.ShouldBe(product.Id);
            deserializedProduct.Name.ShouldBe(product.Name);
            deserializedProduct.IsPublished.ShouldBe(product.IsPublished);
        }
    }
}