using Abp.Modules;
using Abp.Reflection.Extensions;

namespace Abp.Locking.Redis
{
    [DependsOn(typeof(AbpKernelModule), typeof(LockingModule))]
    public class AbpRedisLockModule : AbpModule
    {
        public override void PreInitialize()
        {
            IocManager.Register<AbpRedisLockOptions>();
        }

        public override void Initialize()
        {
            IocManager.RegisterAssemblyByConvention(typeof(AbpRedisLockModule).GetAssembly());
        }
    }
}
