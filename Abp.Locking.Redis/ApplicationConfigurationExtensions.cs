using System;
using Abp.Configuration.Startup;
using Abp.Dependency;

namespace Abp.Locking.Redis
{
    public static class ApplicationConfigurationExtensions
    {
        public static void UseRedisLock(this IAbpStartupConfiguration configuration)
        {
            configuration.UseRedisLock(options => { });
        }

        public static void UseRedisLock(this IAbpStartupConfiguration configuration, Action<AbpRedisLockOptions> configurator)
        {
            var iocManager = configuration.IocManager;

            iocManager.RegisterIfNot<ILockManager, AbpRedisLockManager>(DependencyLifeStyle.Transient);

            configurator(iocManager.Resolve<AbpRedisLockOptions>());
        }
    }
}
