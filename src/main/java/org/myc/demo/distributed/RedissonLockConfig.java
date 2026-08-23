package org.myc.demo.distributed;

import java.util.concurrent.TimeUnit;

/**
 * 分布式锁全局配置（可按环境覆盖）.
 * <p>作为锁模板的默认参数源，所有字段均提供合理默认值，可通过 setter 链式覆盖。
 * 时间类参数（waitTime / leaseTime）的单位统一由 {@link #defaultTimeUnit} 决定。
 *
 * <p>常用默认值：
 * <ul>
 *   <li>keyPrefix = "lock:"</li>
 *   <li>defaultWaitTime = 3（秒），0 表示立即失败</li>
 *   <li>defaultLeaseTime = -1，表示启用看门狗自动续期</li>
 *   <li>sortKeysOnMultiLock = true，多锁场景按 key 排序以降低死锁概率</li>
 * </ul>
 *
 * <p>注意：本配置类采用 fluent 风格（setter 返回 this），不严格遵循 JavaBeans 规范，
 * 不建议直接用于依赖 JavaBeans 内省的框架属性绑定。
 */
public class RedissonLockConfig {
    /**
     * key 统一前缀，最终锁 key = keyPrefix + 业务 key，用于隔离不同系统/环境.
     */
    private String keyPrefix = "lock:";

    /**
     * 默认等待（挂起）时长，单位由 {@link #defaultTimeUnit} 决定。
     * 0 表示立即失败（非阻塞模式）；建议值 0~10 秒
     */
    private long defaultWaitTime = 3L;

    /**
     * 默认持有时长，单位由 {@link #defaultTimeUnit} 决定。
     * <ul>
     *   <li>{@code -1}：启用看门狗自动续期（默认租期 30s，每 10s 续一次，持有锁的线程存活期间不过期）</li>
     *   <li>{@code >0}：固定租期，到期自动释放，不续期</li>
     * </ul>
     * 注意：本配置约定 {@code -1} 启用看门狗，底层须将 {@code -1} 透传为 Redisson 的 leaseTime=-1
     */
    private long defaultLeaseTime = -1L;

    /**
     * 默认时间单位，作用于 {@link #defaultWaitTime} 和 {@link #defaultLeaseTime}.
     */
    private TimeUnit defaultTimeUnit = TimeUnit.SECONDS;

    /**
     * 多锁场景是否对 key 排序。
     * true 时按全局一致顺序加锁，显著降低交叉死锁概率
     */
    private boolean sortKeysOnMultiLock = true;

    /**
     * 是否输出加锁/解锁 debug 信息，用于排查问题时临时打开.
     */
    private boolean verboseLog = false;

    public String getKeyPrefix() { return keyPrefix; }
    public RedissonLockConfig setKeyPrefix(String keyPrefix) { this.keyPrefix = keyPrefix; return this; }

    public long getDefaultWaitTime() { return defaultWaitTime; }
    public RedissonLockConfig setDefaultWaitTime(long v) { this.defaultWaitTime = v; return this; }

    public long getDefaultLeaseTime() { return defaultLeaseTime; }
    public RedissonLockConfig setDefaultLeaseTime(long v) { this.defaultLeaseTime = v; return this; }

    public TimeUnit getDefaultTimeUnit() { return defaultTimeUnit; }
    public RedissonLockConfig setDefaultTimeUnit(TimeUnit v) { this.defaultTimeUnit = v; return this; }

    public boolean isSortKeysOnMultiLock() { return sortKeysOnMultiLock; }
    public RedissonLockConfig setSortKeysOnMultiLock(boolean v) { this.sortKeysOnMultiLock = v; return this; }

    public boolean isVerboseLog() { return verboseLog; }
    public RedissonLockConfig setVerboseLog(boolean v) { this.verboseLog = v; return this; }
}
