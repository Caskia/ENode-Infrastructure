using ECommon.Logging;
using ENode.AggregateSnapshot.Configurations;
using ENode.AggregateSnapshot.Serializers;
using ENode.Domain;
using ENode.Infrastructure;
using System;
using System.Linq;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Threading.Tasks;

namespace ENode.AggregateSnapshot
{
    public class DefaultAggregateSnapshotSaver : IAggregateSnapshotSaver
    {
        private readonly IAggregateSnapshotConfiguration _aggregateSnapshotConfiguration;
        private readonly IAggregateSnapshotSerializer _aggregateSnapshotSerializer;
        private readonly IAggregateSnapshotStore _aggregateSnapshotStore;
        private readonly ILogger _logger;
        private readonly ISubject<AggregateSnapshotStoreContext> _queue = Subject.Synchronize(new Subject<AggregateSnapshotStoreContext>());
        private readonly IRepository _repository;
        private readonly ITypeNameProvider _typeNameProvider;

        public DefaultAggregateSnapshotSaver
        (
            IAggregateSnapshotConfiguration aggregateSnapshotConfiguration,
            IAggregateSnapshotSerializer aggregateSnapshotSerializer,
            IAggregateSnapshotStore aggregateSnapshotStore,
            ILoggerFactory loggerFactory,
            IRepository repository,
            ITypeNameProvider typeNameProvider
        )
        {
            _aggregateSnapshotConfiguration = aggregateSnapshotConfiguration;
            _aggregateSnapshotSerializer = aggregateSnapshotSerializer;
            _aggregateSnapshotStore = aggregateSnapshotStore;
            _logger = loggerFactory.Create(nameof(DefaultAggregateSnapshotSaver));
            _repository = repository;
            _typeNameProvider = typeNameProvider;

            SubscribeAggregateSnapshotStoreQueue();
        }

        public async Task SaveAsync(object aggregateRootId, Type aggregateRootType, int publishedVersion)
        {
            if (aggregateRootId == null)
            {
                throw new ArgumentNullException(nameof(aggregateRootId));
            }

            if (publishedVersion % _aggregateSnapshotConfiguration.VersionInterval != 0)
            {
                return;
            }

            var aggregateRoot = await _repository.GetAsync(aggregateRootType, aggregateRootId);
            if (aggregateRoot == null)
            {
                return;
            }

            await SaveAsync(aggregateRoot, aggregateRootType, publishedVersion);
        }

        public Task SaveAsync(IAggregateRoot aggregateRoot, Type aggregateRootType, int publishedVersion)
        {
            if (aggregateRoot == null)
            {
                throw new ArgumentNullException(nameof(aggregateRoot));
            }

            if (publishedVersion % _aggregateSnapshotConfiguration.VersionInterval != 0)
            {
                return Task.CompletedTask;
            }

            var payload = _aggregateSnapshotSerializer.Serialize(aggregateRoot);

            var context = new AggregateSnapshotStoreContext()
            {
                AggregateRootId = aggregateRoot.UniqueId,
                AggregateRootType = aggregateRootType,
                Payload = payload,
                PublishedVersion = publishedVersion
            };

            _queue.OnNext(context);

            return Task.CompletedTask;
        }

        private void SubscribeAggregateSnapshotStoreQueue()
        {
            _queue
                .GroupByUntil(g => g.AggregateRootId, g => g.Buffer(TimeSpan.FromMilliseconds(_aggregateSnapshotConfiguration.BatchStoreIntervalMilliseconds), _aggregateSnapshotConfiguration.BatchStoreMaximumCumulativeCount))
                .Subscribe(group =>
                {
                    group
                    .ToList()
                    .Select(contexts => Observable.FromAsync(async () =>
                    {
                        if (contexts.Count == 0)
                        {
                            return;
                        }

                        var lastContext = contexts
                            .Aggregate((a, b) => a.PublishedVersion > b.PublishedVersion ? a : b);
                        var aggregateRootTypeName = _typeNameProvider.GetTypeName(lastContext.AggregateRootType);

                        try
                        {
                            await _aggregateSnapshotStore.CreateOrUpdateSnapshotPayloadAsync(lastContext.AggregateRootId, aggregateRootTypeName, lastContext.PublishedVersion, lastContext.Payload);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error($"create or update aggregateRoot[{lastContext.AggregateRootId}] snapshot payload error.", ex);
                        }
                    }))
                    .Concat()
                    .Subscribe();
                });
        }
    }
}