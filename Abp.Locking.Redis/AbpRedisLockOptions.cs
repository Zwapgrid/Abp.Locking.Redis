using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using Abp.Configuration.Startup;
using Abp.Extensions;

namespace Abp.Locking.Redis
{
    // TODO add documentation
    // TODO most of this is copypasta from asp net boileplate AbpRedisCacheOptions
    public class AbpRedisLockOptions
    {
        public IAbpStartupConfiguration AbpStartupConfiguration { get; }

        private const string ConnectionStringKey = "Abp.Redis.Lock.ConnectionString";

        private const string DatabaseIdSettingKey = "Abp.Redis.Lock.DatabaseId";

        public string ConnectionString { get; set; }

        public int DatabaseId { get; set; }

        public TimeSpan DefaultWaitTime { get; set; } = TimeSpan.FromMinutes(1);

        public TimeSpan DefaultExpirityTime { get; set; } = TimeSpan.FromMinutes(10);

        public TimeSpan DefaultRetryTime { get; set; } = TimeSpan.MinValue; // set to min, because it will be automatically reset to RedLock min value

        public AbpRedisLockOptions(IAbpStartupConfiguration abpStartupConfiguration)
        {
            AbpStartupConfiguration = abpStartupConfiguration;

            ConnectionString = GetDefaultConnectionString();
            DatabaseId = GetDefaultDatabaseId();
        }

        private static int GetDefaultDatabaseId()
        {
            var appSetting = ConfigurationManager.AppSettings[DatabaseIdSettingKey];
            if (appSetting.IsNullOrEmpty())
            {
                return -1;
            }

            int databaseId;
            if (!int.TryParse(appSetting, out databaseId))
            {
                return -1;
            }

            return databaseId;
        }

        private static string GetDefaultConnectionString()
        {
            var connStr = ConfigurationManager.ConnectionStrings[ConnectionStringKey];
            if (connStr == null || connStr.ConnectionString.IsNullOrWhiteSpace())
            {
                return "localhost";
            }

            return connStr.ConnectionString;
        }
    }
}
