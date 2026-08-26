package org.myc.demo.example;

import org.myc.demo.distributed.*;
import org.redisson.api.RLock;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Spring Boot 环境下的使用示例（CommandLineRunner 启动时自动执行）.
 *
 * <p>前提：application.yml 中已配置 Redis 地址，
 * {@link org.myc.demo.DemoApplication} 中已注册 DistributedLockUtils Bean.
 */
@Component
public class SpringBootUsageExample implements CommandLineRunner {

    @Override
    public void run(String... args) {
        String orderNo = "SO20240101001";

        // ========== 模式一：全自动（异常上抛，finally 自动解锁）==========
        String ok = DistributedLockUtils.executeWithLock("order:pay:" + orderNo, () -> {
            System.out.println("处理支付...");
            return "支付成功";
        });
        System.out.println("全自动模式结果: " + ok);

        // 全自动·批量多锁
        String transfer = DistributedLockUtils.executeWithMultiLock(
                Arrays.asList("account:A", "account:B"), () -> "转账完成");
        System.out.println("多锁结果: " + transfer);

        // ========== 模式二：免异常（返回 LockResult，绝不抛异常）==========
        LockResult<String> result = DistributedLockUtils.tryExecuteWithLock(
                "order:pay:" + orderNo, 3, 10, TimeUnit.SECONDS, () -> "支付成功");
        System.out.println("免异常模式: " + result);

        // ========== 模式一-b：全自动 + fallback（锁获取失败时降级）==========
        String fallbackResult = DistributedLockUtils.executeWithLockAndFallback(
                "order:pay:" + orderNo,
                () -> {
                    System.out.println("处理支付...");
                    return "支付成功";
                },
                () -> {
                    System.out.println("锁获取失败，降级到本地缓存");
                    return "降级结果";
                });
        System.out.println("全自动+fallback结果: " + fallbackResult);

        // 全自动 + fallback·批量多锁
        String multiFallback = DistributedLockUtils.executeWithMultiLockAndFallback(
                Arrays.asList("account:A", "account:B"),
                () -> "转账完成",
                () -> "降级：稍后重试");
        System.out.println("多锁+fallback结果: " + multiFallback);

        // 全自动 + fallback·感知版（主业务可查 TTL）
        String awareFallback = DistributedLockUtils.executeWithLockAwareAndFallback(
                "order:pay:" + orderNo, 3, 10, TimeUnit.SECONDS,
                (lockHandle, keys) -> {
                    System.out.println("持锁执行, TTL=" + lockHandle.remainTtlToLiveMillis() + "ms");
                    return "感知模式结果";
                },
                () -> "感知模式降级结果");
        System.out.println("感知+fallback结果: " + awareFallback);

        // ========== 模式二-b：免异常 + fallback（锁获取失败时降级，绝不抛异常）==========
        LockResult<String> fallbackLockResult = DistributedLockUtils.tryExecuteWithLockAndFallback(
                "order:pay:" + orderNo, 3, 10, TimeUnit.SECONDS,
                () -> "支付成功",
                () -> "免异常降级结果");
        if (fallbackLockResult.isOk()) {
            System.out.println("免异常+fallback: 主业务成功: " + fallbackLockResult.getData());
        } else if (fallbackLockResult.getData() != null) {
            System.out.println("免异常+fallback: 锁未拿到但fallback成功: " + fallbackLockResult.getData());
        } else {
            System.out.println("免异常+fallback: 全部失败: " + fallbackLockResult.getLockStatus().getDesc());
        }

        // ========== 模式三：半自动（句柄自管，支持 try-with-resources）==========
        try (LockHandle handle = DistributedLockUtils.acquire("stock:deduct:SKU001", 3, 30, TimeUnit.SECONDS)) {
            System.out.println("扣库存..., 剩余TTL=" + handle.remainTtlToLiveMillis() + "ms");
        }

        // 半自动·免异常版（定时任务防重入经典写法）
        LockAcquireResult lockResult = DistributedLockUtils.tryAcquire("job:sync", 0, 60, TimeUnit.SECONDS);
        if (lockResult.isSuccess()) {
            System.out.println("执行定时任务...");
            lockResult.getLockHandle().unlock();
        } else {
            System.out.println("上一轮任务仍在执行, 本轮跳过: " + lockResult.getStatus().getDesc());
        }

        // ========== 模式四：全手动（原生 RLock）==========
        RLock rawLock = DistributedLockUtils.getRawLock("custom:biz");
        rawLock.lock(10, TimeUnit.SECONDS);
        try {
            System.out.println("手动加锁执行业务...");
        } finally {
            DistributedLockUtils.safeUnlock(rawLock);
        }

        System.out.println("===== Spring Boot 分布式锁示例完成 =====");
    }
}
