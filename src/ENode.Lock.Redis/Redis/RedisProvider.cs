using StackExchange.Redis;
using System;

namespace ENode.Lock.Redis
{
    public class RedisProvider
    {
        private readonly Lazy<ConnectionMultiplexer> _connectionMultiplexer;
        private readonly RedisOptions _options;

        /// <summary>
        /// Initializes a new instance of the <see cref="RedisProvider"/> class.
        /// </summary>
        public RedisProvider(RedisOptions options)
        {
            _options = options;
            _connectionMultiplexer = new Lazy<ConnectionMultiplexer>(CreateConnectionMultiplexer);
        }

        /// <summary>
        /// Get connection
        /// </summary>
        public ConnectionMultiplexer ConnectionMultiplexer
        {
            get
            {
                return _connectionMultiplexer.Value;
            }
        }

        /// <summary>
        /// Gets the database connection.
        /// </summary>
        public IDatabase GetDatabase()
        {
            return _connectionMultiplexer.Value.GetDatabase(_options.DatabaseId);
        }

        /// <summary>
        /// Create connection
        /// </summary>
        private ConnectionMultiplexer CreateConnectionMultiplexer()
        {
            return ConnectionMultiplexer.Connect(_options.ConnectionString);
        }
    }
}