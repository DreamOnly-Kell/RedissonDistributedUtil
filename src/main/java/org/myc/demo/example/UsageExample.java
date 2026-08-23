package org.myc.demo.example;

import org.myc.demo.distributed.*;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * 分布式锁工具类 {@link DistributedLockUtils} 的四种使用模式演示.
 *
 * <p>依次展示：全自动（异常上抛）、免异常（返回 LockResult）、半自动（句柄自管）、全手动（原生 RLock），
 * 可直接运行以观察各模式的加锁与解锁行为.
 */
public class UsageExample {
    /**
     * 四种使用模式的演示入口：初始化 RedissonClient 后，依次演示全自动、免异常、半自动、全手动四种模式。
     *
     * @param args 命令行参数（本示例未使用）
     */
    public static void main(String[] args) {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        RedissonClient redissonClient = Redisson.create(config);
        DistributedLockUtils.init(redissonClient);

        String orderNo = "SO20240101001";

        // ========== 模式一：全自动（最常用，异常上抛，finally 必然解锁）==========
        Boolean ok = DistributedLockUtils.executeWithLock("order:pay:" + orderNo, () -> {
            System.out.println("处理支付...");
            return Boolean.TRUE;
        });

        // 全自动·批量多锁（如转账需同时锁定两个账户）
        String s1 = DistributedLockUtils.executeWithMultiLock(Arrays.asList("account:A", "account:B"), () -> {
            return "转账完成";
        });

        // ========== 模式二：免异常（返回 成功/失败/挂起超时，适合旁路容错场景）==========
        LockResult<String> result = DistributedLockUtils.tryExecuteWithLock(
                "order:pay:" + orderNo, 3, 10, TimeUnit.SECONDS, () -> "支付成功");

        switch (result.getLockStatus()) {
            case SUCCESS:
                if (result.isBizSuccess()) {
                    System.out.println("业务结果: " + result.getData());
                } else {
                    System.out.println("拿到锁但业务失败: " + result.getError());
                }
                break;
            case FAILED:      // waitTime=0 时抢不到立即失败
            case TIMEOUT:     // 挂起等待 waitTime 后仍未获得锁
            case INTERRUPTED: // 挂起等待中被中断
                System.out.println("未获得锁: " + result.getLockStatus().getDesc());
                break;
        }

        // ========== 模式三：半自动（拿句柄自己控制，支持 try-with-resources）==========
        try (LockHandle handle = DistributedLockUtils.acquire("stock:deduct:SKU001", 3, 30, TimeUnit.SECONDS)) {
            System.out.println("扣库存..., 剩余TTL=" + handle.remainTtlToLiveMillis() + "ms");
        } // 此处自动解锁

        // 跨方法传递场景：手动 unlock
        LockHandle h = DistributedLockUtils.acquire("task:report:" + orderNo);
        try {
            System.out.println("长任务执行中...");
        } finally {
            h.unlock();
        }

        // 半自动·免异常版：waitTime=0 抢不到立刻跳过（定时任务防重入经典写法）
        LockAcquireResult lockAcquireResult = DistributedLockUtils.tryAcquire("job:sync", 0, 60, TimeUnit.SECONDS);
        if (lockAcquireResult.isSuccess()) {
            try {
                System.out.println("定时任务拿到锁, 开始同步");
            } finally {
                lockAcquireResult.getLockHandle().unlock();
            }
        } else {
            System.out.println("上一轮任务仍在执行, 本轮跳过: " + lockAcquireResult.getStatus().getDesc());
        }

        // ========== 模式四：全手动（原生 RLock 完全自主控制）==========
        RLock rawLock = DistributedLockUtils.getRawLock("custom:biz");
        boolean locked = false;
        try {
            locked = rawLock.tryLock(5, 30, TimeUnit.SECONDS);
            if (locked) {
                System.out.println("完全自主控制的业务逻辑");
            }
        } catch (Exception e) {
            System.out.println("异常.");
        }finally {
            if (locked) {
                DistributedLockUtils.safeUnlock(rawLock);
            }
        }

        // 单锁感知
        String rs = DistributedLockUtils.executeWithLockAware("order:pay:" + orderNo, 3, 10, TimeUnit.SECONDS, (lockHandle, keys) -> {
            System.out.println("当前锁=" + keys + ", 剩余TTL=" + lockHandle.remainTtlToLiveMillis() + "ms");
            if (lockHandle.remainTtlToLiveMillis() < 3000 && !lockHandle.isWatchdogMode()) {
                System.out.println("租期即将耗尽，关注业务耗时，keys=" + keys);
            }
            return "OK";
        });

        // 多锁感知.
        String s = DistributedLockUtils.executeWithMultiLockAware(Arrays.asList("account:B", "account:A"), 5, 30, TimeUnit.SECONDS, (lockHandle, keys) -> {
            System.out.println("持有多把锁: " + keys);
            return "ok";
        });

        redissonClient.shutdown();
    }
}
