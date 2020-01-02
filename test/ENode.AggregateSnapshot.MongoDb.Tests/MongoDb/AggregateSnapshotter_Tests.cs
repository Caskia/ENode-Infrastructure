using ECommon.Components;
using ENode.AggregateSnapshot.Tests.Domain;
using ENode.Domain;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ENode.AggregateSnapshot.Tests.MongoDb
{
    public class AggregateSnapshotter_Tests : TestBase
    {
        private readonly IAggregateSnapshotter _aggregateSnapshotter;
        private readonly ISavableAggregateSnapshotter _savableAggregateSnapshotter;

        public AggregateSnapshotter_Tests()
        {
            _aggregateSnapshotter = ObjectContainer.Resolve<IAggregateSnapshotter>();
            _savableAggregateSnapshotter = ObjectContainer.Resolve<ISavableAggregateSnapshotter>();
        }

        [Fact]
        public async Task InsertOrUpdateSnapshot_Tests()
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
            await _savableAggregateSnapshotter.SaveSnapshotAsync(product, typeof(Product), (product as IAggregateRoot).Version);
        }

        [Fact]
        public async Task SaveAndRestoreSnapshot_Tests()
        {
            //Arrange
            var product = new Product
                (
                    1L,
                    "tomato",
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
            await _savableAggregateSnapshotter.SaveSnapshotAsync(product, typeof(Product), (product as IAggregateRoot).Version);
            var restoredProduct = (await _aggregateSnapshotter.RestoreFromSnapshotAsync(typeof(Product), product.Id.ToString())) as Product;

            //Assert
            restoredProduct.Id.ShouldBe(product.Id);
            restoredProduct.Name.ShouldBe(product.Name);
            restoredProduct.IsPublished.ShouldBe(product.IsPublished);
        }
    }
}