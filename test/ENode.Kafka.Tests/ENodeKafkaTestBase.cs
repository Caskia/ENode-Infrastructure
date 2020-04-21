using ECommon.Components;
using ECommon.Logging;
using ENode.Commanding;
using ENode.Domain;
using ENode.Eventing;
using ENode.Messaging;
using System.Threading.Tasks;
using Xunit;

namespace ENode.Kafka.Tests
{
    [Collection(nameof(ENodeKafkaCollection))]
    public abstract class ENodeKafkaTestBase
    {
        protected IMessagePublisher<IApplicationMessage> _applicationMessagePublisher;
        protected ICommandService _commandService;
        protected IMessagePublisher<DomainEventStreamMessage> _domainEventPublisher;
        protected IMessagePublisher<IDomainException> _domainExceptionPublisher;
        protected IEventStore _eventStore;
        protected ILogger _logger;
        protected IMemoryCache _memoryCache;
        protected IPublishedVersionStore _publishedVersionStore;

        public ENodeKafkaTestBase()
        {
            _commandService = ObjectContainer.Resolve<ICommandService>();
            _memoryCache = ObjectContainer.Resolve<IMemoryCache>();
            _eventStore = ObjectContainer.Resolve<IEventStore>();
            _publishedVersionStore = ObjectContainer.Resolve<IPublishedVersionStore>();
            _domainEventPublisher = ObjectContainer.Resolve<IMessagePublisher<DomainEventStreamMessage>>();
            _applicationMessagePublisher = ObjectContainer.Resolve<IMessagePublisher<IApplicationMessage>>();
            _domainExceptionPublisher = ObjectContainer.Resolve<IMessagePublisher<IDomainException>>();
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(nameof(ENodeKafkaTestBase));
            _logger.Info("----ENode initialized.");
        }

        protected Task<CommandResult> ExecuteCommandAsync(ICommand command)
        {
            return _commandService.ExecuteAsync(command, CommandReturnType.EventHandled);
        }
    }
}