using ECommon.Components;
using ENode.Commanding;
using ENode.Domain;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace ENode.Diagnostics.Tests
{
    public class CommandExecuteContext : ICommandExecuteContext
    {
        private readonly ConcurrentDictionary<string, IAggregateRoot> _aggregateRoots;
        private readonly IRepository _repository;
        private string _result;

        public CommandExecuteContext()
        {
            _repository = ObjectContainer.Resolve<IRepository>();

            _aggregateRoots = new ConcurrentDictionary<string, IAggregateRoot>();
        }

        public void Add(IAggregateRoot aggregateRoot)
        {
            if (aggregateRoot == null)
            {
                throw new ArgumentNullException("aggregateRoot");
            }
            if (!_aggregateRoots.TryAdd(aggregateRoot.UniqueId, aggregateRoot))
            {
                throw new AggregateRootAlreadyExistException(aggregateRoot.UniqueId, aggregateRoot.GetType());
            }
        }

        public Task AddAsync(IAggregateRoot aggregateRoot)
        {
            Add(aggregateRoot);
            return Task.CompletedTask;
        }

        public void Clear()
        {
            _aggregateRoots.Clear();
            _result = null;
        }

        public async Task<T> GetAsync<T>(object id, bool firstFormCache = true) where T : class, IAggregateRoot
        {
            if (id == null)
            {
                throw new ArgumentNullException("id");
            }

            IAggregateRoot aggregateRoot = null;
            if (_aggregateRoots.TryGetValue(id.ToString(), out aggregateRoot))
            {
                return aggregateRoot as T;
            }

            aggregateRoot = await _repository.GetAsync<T>(id);

            if (aggregateRoot != null)
            {
                _aggregateRoots.TryAdd(aggregateRoot.UniqueId, aggregateRoot);
                return aggregateRoot as T;
            }

            return null;
        }

        public string GetResult()
        {
            return _result;
        }

        public IEnumerable<IAggregateRoot> GetTrackedAggregateRoots()
        {
            return _aggregateRoots.Values;
        }

        public Task OnCommandExecutedAsync(CommandResult commandResult)
        {
            return Task.CompletedTask;
        }

        public void SetResult(string result)
        {
            _result = result;
        }
    }
}