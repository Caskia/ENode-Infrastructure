using ECommon.Components;
using ENode.AggregateSnapshot.Tests.Domain;
using ENode.Domain;
using ENode.Infrastructure;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace ENode.AggregateSnapshot.Tests.MongoDb
{
    public class AggregateSnapshotter_Tests : TestBase
    {
        private readonly IAggregateSnapshotSaver _aggregateSnapshotSaver;
        private readonly IAggregateSnapshotStore _aggregateSnapshotStore;
        private readonly IAggregateSnapshotter _aggregateSnapshotter;
        private readonly ITypeNameProvider _typeNameProvider;

        public AggregateSnapshotter_Tests()
        {
            _aggregateSnapshotSaver = ObjectContainer.Resolve<IAggregateSnapshotSaver>();
            _aggregateSnapshotStore = ObjectContainer.Resolve<IAggregateSnapshotStore>();
            _aggregateSnapshotter = ObjectContainer.Resolve<IAggregateSnapshotter>();
            _typeNameProvider = ObjectContainer.Resolve<ITypeNameProvider>();
        }

        [Fact]
        public async Task CreateOrUpdateSnapshot_Tests()
        {
            //Arrange
            var aggregateRootId = "1";
            var payload = "test";

            await _aggregateSnapshotStore.CreateOrUpdateSnapshotPayloadAsync(aggregateRootId, _typeNameProvider.GetTypeName(typeof(Product)), 1, payload);

            //Act
            var storedPayload = await _aggregateSnapshotStore.GetSnapshotPayloadAsync(aggregateRootId);

            //Assert
            storedPayload.ShouldBe(payload);
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
            await _aggregateSnapshotSaver.SaveAsync(product, typeof(Product), (product as IAggregateRoot).Version);
            await Task.Delay(1500);
            var restoredProduct = (await _aggregateSnapshotter.RestoreFromSnapshotAsync(typeof(Product), product.Id.ToString())) as Product;

            //Assert
            restoredProduct.Id.ShouldBe(product.Id);
            restoredProduct.Name.ShouldBe(product.Name);
            restoredProduct.IsPublished.ShouldBe(product.IsPublished);
        }
    }
}