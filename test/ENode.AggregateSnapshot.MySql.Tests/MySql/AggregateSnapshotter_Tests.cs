using ECommon.Components;
using ENode.AggregateSnapshot.Tests.Domain;
using ENode.Domain;
using ENode.Infrastructure;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ENode.AggregateSnapshot.Tests.MySql
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
            var bigName = new StringBuilder();
            for (int i = 0; i < 100000; i++)
            {
                bigName.Append("name");
            }
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
                        new ProductRecord(1L,bigName.ToString(),DateTime.UtcNow),
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

        //[Fact]
        //public async Task SaveSnapshotThreadSafe_Test()
        //{
        //    //Arrange
        //    var product = new Product
        //        (
        //            1L,
        //            "banana",
        //            true,
        //            new List<long>()
        //            {
        //                1L,
        //                2L,
        //                3L
        //            },
        //            new List<ProductRecord>()
        //            {
        //                new ProductRecord(1L,"a",DateTime.UtcNow),
        //                new ProductRecord(2L,"b",DateTime.UtcNow),
        //                new ProductRecord(3L,"c",DateTime.UtcNow),
        //                new ProductRecord(4L,"d",DateTime.UtcNow),
        //            }
        //        );

        //    //Act
        //    var thread = new Thread(() =>
        //    {
        //        while (true)
        //        {
        //            var records = new List<ProductRecord>();
        //            var count = new Random().Next(50000, 80000);
        //            for (int i = 0; i < count; i++)
        //            {
        //                records.Add(new ProductRecord(i, Guid.NewGuid().ToString(), DateTime.UtcNow));
        //            }
        //            product.SetRecords(records);
        //        }
        //    });
        //    thread.Start();

        //    var list = new List<Thread>();
        //    for (int i = 0; i < 10; i++)
        //    {
        //        var saverThread = new Thread(async () =>
        //        {
        //            for (int i = 0; i < 100000; i++)
        //            {
        //                try
        //                {
        //                    await _savableAggregateSnapshotter.SaveSnapshotAsync(product, typeof(Product), (product as IAggregateRoot).Version);
        //                }
        //                catch (Exception ex)
        //                {
        //                }
        //            }
        //        });

        //        list.Add(saverThread);
        //        saverThread.Start();
        //    }

        //    await Task.Delay(30000 * 1000);
        //}
    }
}