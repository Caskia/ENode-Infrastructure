﻿using System;
using System.Threading.Tasks;

namespace ENode.Domain
{
    /// <summary>Represents a repository of the building block of Eric Evans's DDD.
    /// </summary>
    public interface IRepository
    {
        /// <summary>Get an aggregate from memory cache, if not exist, get it from event store.
        /// </summary>
        /// <param name="aggregateRootId"></param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        Task<T> GetAsync<T>(object aggregateRootId) where T : class, IAggregateRoot;
        /// <summary>Get an aggregate from memory cache, if not exist, get it from event store.
        /// </summary>
        /// <param name="aggregateRootType"></param>
        /// <param name="aggregateRootId"></param>
        /// <returns></returns>
        Task<IAggregateRoot> GetAsync(Type aggregateRootType, object aggregateRootId);
    }
}
