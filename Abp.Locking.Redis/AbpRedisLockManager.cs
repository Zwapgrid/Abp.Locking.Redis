using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Abp.Extensions;
using RedLockNet.SERedis;
using RedLockNet.SERedis.Configuration;
using StackExchange.Redis;

namespace Abp.Locking.Redis
{
    public class AbpRedisLockManager : ILockManager
    {
        private const string LockKeyPrefix = "distributedLock:";

        private readonly string _connectionString;
        private readonly int _databaseId; 

        private readonly TimeSpan _waitTime;
        private readonly TimeSpan _expirityTime;
        private readonly TimeSpan _retryTime;

        // TODO this about settings lock factory for several keys (like we have with caching)
        public AbpRedisLockManager(AbpRedisLockOptions options)
        {
            _connectionString = options.ConnectionString;
            _databaseId = options.DatabaseId;

            _waitTime = options.DefaultWaitTime;
            _expirityTime = options.DefaultExpirityTime;
            _retryTime = options.DefaultRetryTime;
        }

        public bool CheckLockSet(string key)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redisDb = connection.GetDatabase(_databaseId);
                return redisDb.KeyExists(GetLockKey(key), CommandFlags.DemandMaster);
            }
        }

        public async Task<bool> CheckLockSetAsync(string key)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redisDb = connection.GetDatabase(_databaseId);
                return await redisDb.KeyExistsAsync(GetLockKey(key), CommandFlags.DemandMaster);
            }
        }

        public void PerformInLock(string key, Action actionTodo)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            PerformInLock(key, actionTodo, expireIn:null, cancellationToken: default);
        }

        public void PerformInLock(string key, Action actionTodo, TimeSpan? expireIn = null)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            PerformInLock(key, actionTodo, expireIn: expireIn, cancellationToken: default);
        }

        public void PerformInLock(string key, Action actionTodo, CancellationToken cancellationToken = default)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            PerformInLock(key, actionTodo, expireIn:null, cancellationToken: cancellationToken);
        }

        private void PerformInLock(string key, Action actionTodo, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            expireIn = expireIn ?? _expirityTime;

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redLockMultiplexer = (RedLockMultiplexer)connection;
                redLockMultiplexer.RedisDatabase = _databaseId;
                using (var factory = RedLockFactory.Create(new List<RedLockMultiplexer> { connection }, null))
                {
                    var resource = GetLockKey(key);

                    using (factory.CreateLock(resource, expiryTime: expireIn.Value, waitTime: _waitTime, retryTime: _retryTime, cancellationToken: cancellationToken))
                    {
                        actionTodo();
                    }
                }
            }
        }

        public TResult PerformInLock<TResult>(string key, Func<TResult> actionTodo)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLock(key, actionTodo, expireIn:null, cancellationToken: default);
        }

        public TResult PerformInLock<TResult>(string key, Func<TResult> actionTodo, TimeSpan? expireIn = null)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLock(key, actionTodo, expireIn: expireIn, cancellationToken: default);
        }

        public TResult PerformInLock<TResult>(string key, Func<TResult> actionTodo, CancellationToken cancellationToken = default)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLock(key, actionTodo, expireIn: null, cancellationToken: cancellationToken);
        }

        private TResult PerformInLock<TResult>(string key, Func<TResult> actionTodo, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            expireIn = expireIn ?? _expirityTime;

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redLockMultiplexer = (RedLockMultiplexer)connection;
                redLockMultiplexer.RedisDatabase = _databaseId;
                using (var factory = RedLockFactory.Create(new List<RedLockMultiplexer> { connection }, null))
                {
                    var resource = GetLockKey(key);

                    using (factory.CreateLock(resource, expiryTime: expireIn.Value, waitTime: _waitTime, retryTime: _retryTime, cancellationToken: cancellationToken))
                    {
                        return actionTodo();
                    }
                }
            }
        }

        public Task PerformInLockAsync(string key, Func<Task> actionTodo)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn:null, cancellationToken: default);
        }

        public Task PerformInLockAsync(string key, Func<Task> actionTodo, TimeSpan? expireIn = null)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn: expireIn, cancellationToken: default);
        }

        public Task PerformInLockAsync(string key, Func<Task> actionTodo, CancellationToken cancellationToken = default)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn:null, cancellationToken: cancellationToken);
        }

        private async Task PerformInLockAsync(string key, Func<Task> actionTodo, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            expireIn = expireIn ?? _expirityTime;

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redLockMultiplexer = (RedLockMultiplexer)connection;
                redLockMultiplexer.RedisDatabase = _databaseId;
                using (var factory = RedLockFactory.Create(new List<RedLockMultiplexer> { connection }, null))
                {
                    var resource = GetLockKey(key);

                    using (await factory.CreateLockAsync(resource, expiryTime: expireIn.Value, waitTime: _waitTime, retryTime: _retryTime, cancellationToken: cancellationToken))
                    {
                        await actionTodo();
                    }
                }
            }
        }

        public Task<TResult> PerformInLockAsync<TResult>(string key, Func<Task<TResult>> actionTodo)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn: null, cancellationToken: default);
        }

        public Task<TResult> PerformInLockAsync<TResult>(string key, Func<Task<TResult>> actionTodo, TimeSpan? expireIn = null)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn: expireIn, cancellationToken: default);
        }

        public Task<TResult> PerformInLockAsync<TResult>(string key, Func<Task<TResult>> actionTodo, CancellationToken cancellationToken = default)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            return PerformInLockAsync(key, actionTodo, expireIn:null, cancellationToken: cancellationToken);
        }

        private async Task<TResult> PerformInLockAsync<TResult>(string key, Func<Task<TResult>> actionTodo, TimeSpan? expireIn, CancellationToken cancellationToken)
        {
            if (key.IsNullOrEmpty())
                throw new ArgumentNullException(nameof(key));
            if (actionTodo == null)
                throw new ArgumentNullException(nameof(actionTodo));
            expireIn = expireIn ?? _expirityTime;

            using (var connection = ConnectionMultiplexer.Connect(_connectionString))
            {
                var redLockMultiplexer = (RedLockMultiplexer)connection;
                redLockMultiplexer.RedisDatabase = _databaseId;
                using (var factory = RedLockFactory.Create(new List<RedLockMultiplexer> { connection }, null))
                {
                    var resource = GetLockKey(key);

                    using (await factory.CreateLockAsync(resource, expiryTime: expireIn.Value, waitTime: _waitTime, retryTime: _retryTime, cancellationToken: cancellationToken))
                    {
                        return await actionTodo();
                    }
                }
            }
        }

        private string GetLockKey(string key) => $"{LockKeyPrefix}{key}";
    }
}
