package org.myc.demo.distributed;

import org.redisson.api.RLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 已成功持有的分布式锁句柄（单锁 / 多锁通用）。
 *
 * <p>两种释放方式：
 * <ul>
 *   <li>手动调用 {@link #unlock()}</li>
 *   <li>try-with-resources：{@code try (LockHandle h = DistributedLockUtils.acquire("k")) { ... }} 自动释放</li>
 * </ul>
 *
 * <p>注意：必须由【加锁线程】释放（Redisson 按 threadId 校验持有权）。
 */
public class LockHandle implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(LockHandle.class);

    /**
     * 本次加锁涉及的全部 key（含统一前缀，不可变）
     */
    private final List<String> keys;
    /**
     * 底层原生 RLock 列表（单锁 size=1，多锁为各成员锁）
     */
    private final List<RLock> locks;
    /**
     * 组合锁对象（单锁时即该锁本身，多锁时为 RedissonMultiLock）
     */
    private final RLock combinedLock;
    /**
     * 预期持有时长（毫秒）；看门狗模式为 -1
     */
    private final long leaseMillis;
    /**
     * 加锁成功的时间戳（毫秒），用于计算实际持锁耗时
     */
    private final long acquiredAtMillis;
    /**
     * 释放状态标记（AtomicBoolean 保证 unlock 幂等）
     */
    private final AtomicBoolean released = new AtomicBoolean(false);

    LockHandle(List<String> keys, List<RLock> locks, RLock combinedLock, long leaseMillis) {
        this.keys = Collections.unmodifiableList(keys);
        this.locks  = Collections.unmodifiableList(locks);
        this.combinedLock = combinedLock;
        this.leaseMillis = leaseMillis;
        this.acquiredAtMillis = System.currentTimeMillis();
    }

    /**
     * 参与本次加锁的全部 key（已含统一前缀，不可变列表）.
     *
     * @return 本次加锁涉及的全部 key
     */
    public List<String> getKeys() {
        return keys;
    }

    /**
     * 底层原生锁列表（高级场景使用）.
     *
     * @return 原生 RLock 列表
     */
    public List<RLock> getRawLocks() {
        return locks;
    }

    /**
     * 组合锁对象（单锁时即该锁本身）.
     *
     * @return 组合锁
     */
    public RLock getCombinedLock() {
        return combinedLock;
    }

    /**
     * 是否为看门狗自动续期模式.
     *
     * @return leaseMillis<=0 时为 true（看门狗续期）；否则 false（固定租期）
     */
    public boolean isWatchdogMode() {
        return leaseMillis <= 0;
    }

    /**
     * 预期持有时长（毫秒）；看门狗模式返回 -1.
     *
     * @return 租期毫秒数（看门狗模式为 -1）
     */
    public long getLeaseMillis() {
        return leaseMillis;
    }

    /**
     * 当前线程是否仍持有【全部】锁.
     *
     * @return 全部锁均被当前线程持有时为 true
     */
    public boolean isHeldByCurrentThread() {
        long tid = Thread.currentThread().getId();
        for (RLock lock : locks) {
            if (!lock.isHeldByThread(tid)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 第一把锁的剩余存活时间（毫秒），便于监控告警；异常返回 -2.
     *
     * @return 剩余 TTL（毫秒）；查询异常时返回 -2
     */
    public long remainTtlToLiveMillis() {
        try {
            return locks.get(0).remainTimeToLive();
        } catch (Exception e) {
            return -2L;
        }
    }

    /**
     * 释放锁（幂等，可安全重复调用）.
     * <p>只释放当前线程仍持有的锁：锁已过期等情况仅告警不抛出异常.
     */
    public void unlock() {
        if (!released.compareAndSet(false, true)) {
            logger.debug("[RedissonLock] 重复解锁被忽略，keys={}", keys);
            return;
        }
        long threadId = Thread.currentThread().getId();
        for (RLock lock : locks) {
            try {
                if (lock.isHeldByThread(threadId)) {
                    lock.unlock();
                } else {
                    logger.warn("[RedissonLock] 锁已不属于当前线程(可能已过期自动释放)，跳过解锁，key={}", lock.getName());
                }
            } catch (Exception e) {
                // 典型场景：isHeldByThread 与 unlock 执行之间锁恰好过期 -> IllegalMonitorStateException
                logger.warn("[RedissonLock] 解锁异常(通常为锁已过期), key={}", lock.getName(), e);
            }
        }
    }

    /**
     * 释放锁，等价于 {@link #unlock()}，支持 try-with-resources.
     */
    @Override
    public void close() {
        unlock();
    }

    /**
     * toString重写.
     */
    @Override
    public String toString() {
        return "LockHandle{keys=" +  keys + ", watchdog=" + isWatchdogMode() + ", leaseMillis=" +
                leaseMillis + ", heldMillis=" + (System.currentTimeMillis() - acquiredAtMillis) + '}';
    }
}
