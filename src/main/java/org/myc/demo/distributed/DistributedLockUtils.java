package org.myc.demo.distributed;

import org.omg.CORBA.Object;
import org.redisson.RedissonMultiLock;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redisson 的生产级分布式锁工具类。
 *
 * <p>提供六种使用模式，按「自动化程度」由高到低排列：
 * <ul>
 *   <li><b>全自动</b>：{@link #executeWithLock(String, LockCallback)} 系列 —— 加锁 → 执行 → finally 自动解锁，业务异常原样上抛</li>
 *   <li><b>全自动 + fallback</b>：{@link #executeWithLockAndFallback(String, LockCallback, LockCallback)} 系列 —— 锁获取失败时执行降级回调</li>
 *   <li><b>免异常</b>：{@code tryExecuteWithLock} 系列 —— 不抛异常，返回 {@link LockResult}（成功 / 失败 / 挂起超时）</li>
 *   <li><b>免异常 + fallback</b>：{@code tryExecuteWithLockAndFallback} 系列 —— 不抛异常，锁获取失败时执行降级回调</li>
 *   <li><b>半自动</b>：{@link #acquire(String, long, long, TimeUnit)} 系列 —— 返回 {@link LockHandle} 句柄，调用方自行释放（支持 try-with-resources）</li>
 *   <li><b>全手动</b>：{@code getRawLock} / {@code buildMultiLock} 系列 —— 返回原生 {@code RLock}，生命周期完全自理</li>
 * </ul>
 *
 * <p>初始化方式（二选一）：
 * <ul>
 *   <li>非 Spring：{@code DistributedLockUtils.init(redissonClient)}</li>
 *   <li>Spring：注册本类为 Bean 并注入 RedissonClient（构造器自动完成静态初始化）</li>
 * </ul>
 */
public class DistributedLockUtils {

    private static final Logger log = LoggerFactory.getLogger(DistributedLockUtils.class);

    /**
     * 持锁耗时超过该阈值则打告警日志（ms），便于发现锁力度过大问题
     */
    private static final long SLOW_COST_WARN_MILLIS = 1000L;
    /**
     * Redisson 客户端（静态持有，由 init 注入）.
     */
    private static volatile RedissonClient CLIENT;
    /**
     * 全局锁配置（默认参数源，可被 setConfig 覆盖）.
     */
    private static volatile RedissonLockConfig CONFIG = new RedissonLockConfig();

    /**
     * Spring场景，注入 RedissonClient 后注册为Bean，同时完成静态初始化
     */
    public DistributedLockUtils(RedissonClient redissonClient) {
        init(redissonClient);
    }

    /**
     * 非 Spring 场景：应用启动时手动初始化一次.
     * @param redissonClient Redisson客户端
     */
    public static void init(RedissonClient redissonClient) {
        Objects.requireNonNull(redissonClient, "RedissonClient 不能为空");
        CLIENT = redissonClient;
        log.info("[RedissonLock] DistributedLockUtils 初始化完成");
    }

    /**
     * 返回已初始化的 RedissonClient；未初始化时抛 IllegalStateException.
     *
     * @return Redisson 客户端
     * @throws IllegalStateException 尚未调用 init(redissonClient) 时
     */
    private static RedissonClient client() {
        RedissonClient client = CLIENT;
        if (client == null) {
            throw new IllegalStateException("DistributedLockUtils 尚未初始化，请先调用init(redissonClient)");
        }
        return client;
    }

    /**
     * 获取全局锁配置（默认参数源，可被 setConfig 覆盖）.
     *
     * @return 当前 RedissonLockConfig
     */
    public static RedissonLockConfig getConfig() {
        return CONFIG;
    }

    /**
     * 设置全局锁配置（传入 null 时回退为默认配置）.
     *
     * @param config 锁配置
     */
    public static void setConfig(RedissonLockConfig config) {
        CONFIG = (config == null ? new RedissonLockConfig() : config);
    }

    /* ================================================================
     * 一、全自动模式：加锁 -> 执行 -> finally 解锁，业务异常原样上抛
     * ================================================================ */

    /**
     * 全自动·无返回值·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁后执行锁内业务。
     * 锁内业务异常原样上抛（受检异常包装为 RuntimeException）。
     *
     * @param key    业务锁 key
     * @param action 锁内业务
     */
    public static void runWithLock(String key, Runnable action) {
        runWithLock(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit(), action);
    }

    /**
     * 全自动·无返回值·完整参数：调用方指定等待时长与租期加锁运行.
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param action    锁内业务
     */
    public static void runWithLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit, Runnable action) {
        executeWithLock(key, waitTime, leaseTime, timeUnit, () -> {
            action.run(); return null;
        });
    }

    /**
     * 全自动·有返回值·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁后执行锁内业务并返回其结果。
     * 锁内业务异常原样上抛（受检异常包装为 RuntimeException）。
     *
     * @param key      业务锁 key
     * @param callback 锁内业务
     * @param <T>      业务返回值类型
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithLock(String key, LockCallback<T> callback) {
        return executeWithLock(key , CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback);
    }

    /**
     * 全自动·有返回值·完整参数
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit, LockCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> keys = buildSingleKeys(key);
        LockHandle handle = doAcquire(keys, waitTime, leaseTime, timeUnit);
        long begin = System.currentTimeMillis();
        try {
            return invokeBusiness(callback, keys);
        } finally {
            warnIfSlow(keys, begin);
            handle.unlock();
        }
    }

    /**
     * 全自动·回调可感知锁句柄：业务内可查询剩余 TTL、持有状态等，用于决定是否续期/告警.
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务（可感知 LockHandle 与 keys；注意不要在回调内手动 unlock）
     * @param <T>       业务返回值类型
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithLockAware(String key, long waitTime, long leaseTime, TimeUnit timeUnit, LockAwareCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> keys = buildSingleKeys(key);
        LockHandle handle = doAcquire(keys, waitTime, leaseTime, timeUnit);
        long begin = System.currentTimeMillis();
        try {
            return invokeAwareBusiness(callback, handle, keys);
        } finally {
            warnIfSlow(keys, begin);
            handle.unlock();
        }
    }

    /**
     * 全自动·批量多锁·回调可感知锁句柄（如查询剩余 TTL 决定是否续期/告警）.
     * 多把锁要么全加上、要么全不加（原子性由 RedissonMultiLock 保证），内部默认对 key 排序防交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务（可感知 LockHandle 与 keys；注意不要在回调内手动 unlock）
     * @param <T>       业务返回值类型
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithMultiLockAware(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit, LockAwareCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> builtKeys = buildMultiKeys(keys);
        LockHandle handle = doAcquire(builtKeys, waitTime, leaseTime, timeUnit);
        long begin = System.currentTimeMillis();
        try {
            return invokeAwareBusiness(callback, handle, builtKeys);
        } finally {
            warnIfSlow(builtKeys, begin);
            handle.unlock();
        }
    }


    /**
     * 全自动·批量多锁·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁后执行锁内业务并返回其结果。
     * 多把锁要么全加上、要么全不加（原子性由 RedissonMultiLock 保证），内部默认对 key 排序防交叉死锁。
     *
     * @param keys      业务锁 key 集合
     * @param callback  锁内业务
     * @param <T>       业务返回值类型
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithMultiLock(Collection<String> keys, LockCallback<T> callback) {
        return executeWithMultiLock(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit(), callback);
    }

    /**
     * 全自动·批量多锁·完整参数.
     * <p>多把锁要么全加上、要么全不加（原子性由 RedissonMultiLock 保证）；内部默认对 key 排序，保证全局一致的加锁顺序，避免交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务
     * @param <T>       业务返回值类型
     * @return 业务返回值
     * @throws LockAcquireException 未获得锁
     * @throws RuntimeException     业务异常（RuntimeException 原样抛出，受检异常会被包装）
     */
    public static <T> T executeWithMultiLock(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit, LockCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> builtKeys = buildMultiKeys(keys);
        LockHandle handle = doAcquire(builtKeys, waitTime, leaseTime, timeUnit);
        long begin = System.currentTimeMillis();
        try {
            return invokeBusiness(callback, builtKeys);
        } finally {
            warnIfSlow(builtKeys, begin);
            handle.unlock();
        }
    }

    /**
     * 全自动·批量多锁·无返回值：多把锁要么全加上、要么全不加，内部默认对 key 排序防交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param action    锁内业务
     */
    public static void runWithMultiLock(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit, Runnable action) {
        executeWithMultiLock(keys, waitTime, leaseTime, timeUnit, () -> { action.run(); return null; });
    }

    /* ================================================================
     * 一-b、全自动 + fallback：锁获取失败时执行降级回调，而非直接抛异常
     * ================================================================ */

    /**
     * 全自动 + fallback·单锁·默认参数：锁获取成功执行主业务，失败执行 fallback.
     *
     * @param key      业务锁 key
     * @param callback 主业务（锁获取成功时执行）
     * @param fallback 降级业务（锁获取失败时执行；可为 null 则等同于无 fallback）
     * @param <T>      业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithLockAndFallback(String key, LockCallback<T> callback, LockCallback<T> fallback) {
        return executeWithLockAndFallback(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 全自动 + fallback·单锁·完整参数.
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithLockAndFallback(String key, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                   LockCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        return doAcquireOrFallback(buildSingleKeys(key), waitTime, leaseTime, timeUnit, callback, fallback);
    }

    /**
     * 全自动 + fallback·单锁·无返回值·默认参数.
     *
     * @param key      业务锁 key
     * @param action   主业务
     * @param fallback 降级业务（可为 null）
     */
    public static void runWithLockAndFallback(String key, Runnable action, Runnable fallback) {
        runWithLockAndFallback(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), action, fallback);
    }

    /**
     * 全自动 + fallback·单锁·无返回值·完整参数.
     *
     * @param key       业务锁 key
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param action    主业务
     * @param fallback  降级业务（可为 null）
     */
    public static void runWithLockAndFallback(String key, long waitTime, long leaseTime, TimeUnit timeUnit,
                                              Runnable action, Runnable fallback) {
        executeWithLockAndFallback(key, waitTime, leaseTime, timeUnit,
                () -> { action.run(); return null; },
                fallback == null ? null : () -> { fallback.run(); return null; });
    }

    /**
     * 全自动 + fallback·批量多锁·默认参数.
     *
     * @param keys      业务锁 key 集合
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithMultiLockAndFallback(Collection<String> keys, LockCallback<T> callback, LockCallback<T> fallback) {
        return executeWithMultiLockAndFallback(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 全自动 + fallback·批量多锁·完整参数.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithMultiLockAndFallback(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                        LockCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        return doAcquireOrFallback(buildMultiKeys(keys), waitTime, leaseTime, timeUnit, callback, fallback);
    }

    /**
     * 全自动 + fallback·批量多锁·无返回值·完整参数.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param action    主业务
     * @param fallback  降级业务（可为 null）
     */
    public static void runWithMultiLockAndFallback(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                   Runnable action, Runnable fallback) {
        executeWithMultiLockAndFallback(keys, waitTime, leaseTime, timeUnit,
                () -> { action.run(); return null; },
                fallback == null ? null : () -> { fallback.run(); return null; });
    }

    /**
     * 全自动 + fallback·单锁感知版·默认参数：主业务可感知锁句柄.
     *
     * @param key      业务锁 key
     * @param callback 主业务（可感知 LockHandle 与 keys）
     * @param fallback 降级业务（不可感知锁，可为 null）
     * @param <T>      业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithLockAwareAndFallback(String key, LockAwareCallback<T> callback, LockCallback<T> fallback) {
        return executeWithLockAwareAndFallback(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 全自动 + fallback·单锁感知版·完整参数：主业务可感知锁句柄.
     *
     * @param key       业务锁 key
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务（可感知 LockHandle 与 keys）
     * @param fallback  降级业务（不可感知锁，可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithLockAwareAndFallback(String key, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                        LockAwareCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> keys = buildSingleKeys(key);
        LockAcquireResult result = tryAcquireInternal(keys, waitTime, leaseTime, timeUnit);
        if (result.isSuccess()) {
            long begin = System.currentTimeMillis();
            try {
                return invokeAwareBusiness(callback, result.getLockHandle(), keys);
            } finally {
                warnIfSlow(keys, begin);
                result.getLockHandle().unlock();
            }
        }
        if (fallback != null) {
            log.info("[RedissonLock] 获取锁失败, 执行 fallback, keys={}", keys);
            try {
                return fallback.execute();
            } catch (RuntimeException re) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), re);
            } catch (Exception e) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), e);
            }
        }
        throw new LockAcquireException(result.getStatus(), result.getLockKey());
    }

    /**
     * 全自动 + fallback·批量多锁感知版·默认参数：主业务可感知锁句柄.
     *
     * @param keys      业务锁 key 集合
     * @param callback  主业务（可感知 LockHandle 与 keys）
     * @param fallback  降级业务（不可感知锁，可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithMultiLockAwareAndFallback(Collection<String> keys, LockAwareCallback<T> callback, LockCallback<T> fallback) {
        return executeWithMultiLockAwareAndFallback(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 全自动 + fallback·批量多锁感知版·完整参数：主业务可感知锁句柄.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务（可感知 LockHandle 与 keys）
     * @param fallback  降级业务（不可感知锁，可为 null）
     * @param <T>       业务返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也抛异常时
     */
    public static <T> T executeWithMultiLockAwareAndFallback(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                             LockAwareCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> builtKeys = buildMultiKeys(keys);
        LockAcquireResult result = tryAcquireInternal(builtKeys, waitTime, leaseTime, timeUnit);
        if (result.isSuccess()) {
            long begin = System.currentTimeMillis();
            try {
                return invokeAwareBusiness(callback, result.getLockHandle(), builtKeys);
            } finally {
                warnIfSlow(builtKeys, begin);
                result.getLockHandle().unlock();
            }
        }
        if (fallback != null) {
            log.info("[RedissonLock] 获取锁失败, 执行 fallback, keys={}", builtKeys);
            try {
                return fallback.execute();
            } catch (RuntimeException re) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), re);
            } catch (Exception e) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), e);
            }
        }
        throw new LockAcquireException(result.getStatus(), result.getLockKey());
    }

    /* ================================================================
     * 二、免异常模式：任何情况都不抛异常，通过 LockResult 返回 成功/失败/挂起超时
     * ================================================================ */

    /**
     * 免异常·单锁·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁执行，任何情况都不抛异常。
     * 结果统一封装为 {@link LockResult}（成功 / 失败 / 挂起超时）。
     *
     * @param key      业务锁 key
     * @param callback 锁内业务
     * @param <T>      业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithLock(String key, LockCallback<T> callback) {
        return tryExecuteWithLock(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit(), callback);
    }

    /**
     * 免异常·单锁·完整参数：任何情况都不抛异常，通过 {@link LockResult} 返回 （成功 / 失败 / 挂起超时）
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithLock(String key, long waitTime, long leaseTime, TimeUnit timeUnit, LockCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> keys = buildSingleKeys(key);
        LockAcquireResult lockAcquireResult = tryAcquireInternal(keys, waitTime, leaseTime, timeUnit);
        if (!lockAcquireResult.isSuccess()) {
            return LockResult.lockFail(lockAcquireResult.getStatus(), lockAcquireResult.getLockKey());
        }

        long begin = System.currentTimeMillis();
        try {
            T data = callback.execute();
            return LockResult.success(data, lockAcquireResult.getLockKey(), cost(begin));
        } catch (Exception bizException) {
            log.error("[RedissonLock] 锁内业务执行异常， keys={}", keys, bizException);
            return LockResult.bizError(lockAcquireResult.getLockKey(), cost(begin), bizException);
        } finally {
            lockAcquireResult.getLockHandle().unlock();
        }
    }

    /**
     * 免异常·批量多锁·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁执行多锁业务，任何情况都不抛异常。
     * 多把锁要么全加上、要么全不加（原子性由 RedissonMultiLock 保证），内部默认对 key 排序防交叉死锁。
     *
     * @param keys      业务锁 key 集合
     * @param callback  锁内业务
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithMultiLock(Collection<String> keys, LockCallback<T> callback) {
        return tryExecuteWithMultiLock(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit(), callback);
    }

    /**
     * 免异常·批量多锁·完整参数：任何情况都不抛异常，通过 {@link LockResult} 返回 （成功 / 失败 / 挂起超时）
     * 多把锁要么全加上、要么全不加，内部默认对 key 排序防交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @param callback  锁内业务
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithMultiLock(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit, LockCallback<T> callback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        List<String> builtKeys = buildMultiKeys(keys);
        LockAcquireResult lockAcquireResult = tryAcquireInternal(builtKeys, waitTime, leaseTime, timeUnit);
        if (!lockAcquireResult.isSuccess()) {
            return LockResult.lockFail(lockAcquireResult.getStatus(), lockAcquireResult.getLockKey());
        }
        long begin = System.currentTimeMillis();
        try {
            T data = callback.execute();
            return LockResult.success(data, lockAcquireResult.getLockKey(), cost(begin));
        } catch (Exception bizException) {
            log.error("[RedissonLock] 锁内业务执行异常, keys={}", keys, bizException);
            return LockResult.bizError(lockAcquireResult.getLockKey(), cost(begin), bizException);
        } finally {
            lockAcquireResult.getLockHandle().unlock();
        }
    }

    /* ================================================================
     * 二-b、免异常 + fallback：锁获取失败时执行降级回调，结果封装为 LockResult
     * ================================================================ */

    /**
     * 免异常 + fallback·单锁·默认参数：锁获取成功执行主业务，失败执行 fallback，任何情况都不抛异常.
     *
     * @param key      业务锁 key
     * @param callback 主业务
     * @param fallback 降级业务（可为 null）
     * @param <T>      业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithLockAndFallback(String key, LockCallback<T> callback, LockCallback<T> fallback) {
        return tryExecuteWithLockAndFallback(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 免异常 + fallback·单锁·完整参数.
     *
     * @param key       业务锁 key
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithLockAndFallback(String key, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                                   LockCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        return doAcquireOrFallbackExceptionFree(buildSingleKeys(key), waitTime, leaseTime, timeUnit, callback, fallback);
    }

    /**
     * 免异常 + fallback·批量多锁·默认参数.
     *
     * @param keys      业务锁 key 集合
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithMultiLockAndFallback(Collection<String> keys, LockCallback<T> callback, LockCallback<T> fallback) {
        return tryExecuteWithMultiLockAndFallback(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(),
                CONFIG.getDefaultTimeUnit(), callback, fallback);
    }

    /**
     * 免异常 + fallback·批量多锁·完整参数.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务
     * @param fallback  降级业务（可为 null）
     * @param <T>       业务返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    public static <T> LockResult<T> tryExecuteWithMultiLockAndFallback(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                                       LockCallback<T> callback, LockCallback<T> fallback) {
        Objects.requireNonNull(callback, "callback 不能为空");
        return doAcquireOrFallbackExceptionFree(buildMultiKeys(keys), waitTime, leaseTime, timeUnit, callback, fallback);
    }
    /* ================================================================
     * 三、半自动模式：只负责加锁，返回句柄交由调用方自行使用和释放
     * ================================================================ */

    /**
     * 半自动·抛异常版·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁，失败抛 {@link LockAcquireException}。
     * 获取后务必配合 try-with-resources 或 finally 解锁。
     *
     * @param key 业务锁 key
     * @return 已加锁的句柄（用毕务必 unlock）
     */
    public static LockHandle acquire(String key) {
        return acquire(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit());
    }

    /**
     * 半自动·抛异常版（完整参数）：失败抛 {@link LockAcquireException}。务必配合 try-with-resources 或 finally 解锁.
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 已加锁的句柄（用毕务必 unlock）
     */
    public static LockHandle acquire(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        return doAcquire(buildSingleKeys(key), waitTime, leaseTime, timeUnit);
    }

    /**
     * 半自动·批量多锁·抛异常版·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁多锁，失败抛 {@link LockAcquireException}。
     * 内部默认对 key 排序防交叉死锁；获取后务必配合 try-with-resources 或 finally 解锁。
     *
     * @param keys 业务锁 key 集合
     * @return 已加锁的句柄（用毕务必 unlock）
     */
    public static LockHandle acquireMulti(Collection<String> keys) {
        return acquireMulti(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit());
    }

    /**
     * 半自动·批量多锁·抛异常版（完整参数）：成功返回句柄，失败抛 {@link LockAcquireException}。内部默认对 key 排序防交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 已加锁的句柄（用毕务必 unlock）
     */
    public static LockHandle acquireMulti(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit) {
        return doAcquire(buildMultiKeys(keys), waitTime, leaseTime, timeUnit);
    }

    /**
     * 半自动·免异常版·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁，失败只返回状态不抛异常。
     *
     * @param key 业务锁 key
     * @return 获取结果（成功时含句柄，失败时仅含状态）
     */
    public static LockAcquireResult tryAcquire(String key) {
        return tryAcquire(key, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit());
    }

    /**
     * 半自动·免异常版（完整参数）：成功返回含句柄的结果，失败只返回状态不抛异常.
     *
     * @param key       业务锁 key
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 获取结果（成功时含句柄，失败时仅含状态）
     */
    public static LockAcquireResult tryAcquire(String key, long waitTime, long leaseTime, TimeUnit timeUnit) {
        return tryAcquireInternal(buildSingleKeys(key), waitTime, leaseTime, timeUnit);
    }

    /**
     * 半自动·批量多锁·免异常版·默认参数：使用全局配置的等待时长、租期与 TimeUnit 加锁多锁，失败只返回状态不抛异常。
     * 内部默认对 key 排序防交叉死锁。
     *
     * @param keys 业务锁 key 集合
     * @return 获取结果（成功时含句柄，失败时仅含状态）
     */
    public static LockAcquireResult tryAcquireMulti(Collection<String> keys) {
        return tryAcquireMulti(keys, CONFIG.getDefaultWaitTime(), CONFIG.getDefaultLeaseTime(), CONFIG.getDefaultTimeUnit());
    }

    /**
     * 半自动·批量多锁·免异常版（完整参数）：成功返回含句柄的结果，失败只返回状态不抛异常。内部默认对 key 排序防交叉死锁.
     *
     * @param keys      业务锁 key 集合
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 获取结果（成功时含句柄，失败时仅含状态）
     */
    public static LockAcquireResult tryAcquireMulti(Collection<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit) {
        return tryAcquireInternal(buildMultiKeys(keys), waitTime, leaseTime, timeUnit);
    }

    /* ================================================================
     * 四、全手动模式：直接操作原生 RLock，加锁/解锁/续期完全自理
     * ================================================================ */

    /**
     * 获取原生 RLock（已拼接统一前缀），加锁/解锁/续期完全由调用方控制。
     *
     * @param key 业务锁 key
     * @return 原生 RLock 实例（未加锁，需自行调用 lock/unlock）
     */
    public static RLock getRawLock(String key) {
        return client().getLock(buildKey(key));
    }

    /**
     * 构建原生多锁（内部已排序防死锁），加锁/解锁/续期由调用方处理。
     *
     * @param keys 业务锁 key 集合
     * @return 原生 RedissonMultiLock 实例（未加锁，需自行调用 lock/unlock）
     */
    public static RedissonMultiLock buildMultiLock(Collection<String> keys) {
        List<String> built = buildMultiKeys(keys);
        RLock[] locks = new RLock[built.size()];
        for (int i = 0; i < built.size(); i++) {
            locks[i] = client().getLock(built.get(i));
        }
        return new RedissonMultiLock(locks);
    }

    /**
     * 手动模式的安全解锁工具：幂等 + 校验持有权 + 吞掉"锁已过期"异常。
     * <ul>
     *   <li>锁为 null 时直接跳过</li>
     *   <li>锁不属于当前线程（通常已过期）仅告警不抛异常</li>
     *   <li>解锁过程中的其他异常同样降级为告警，不会向上传播</li>
     * </ul>
     *
     * @param lock 待解锁的原生 RLock（可空）
     */
    public static void safeUnlock(RLock lock) {
        if (lock == null) {
            return;
        }
        try {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            } else {
                log.warn("[RedissonLock] 手动解锁时发现锁不属于当前线程(可能已过期), key={}", lock.getName());
            }
        } catch (Throwable e) {
            log.warn("[RedissonLock] 手动解锁异常(通常为锁已过期), key={}", lock.getName(), e);
        }
    }

    /* ================================================================
     * 内部实现
     * ================================================================ */

    /**
     * 加锁并兜底为句柄：调用 {@link #tryAcquireInternal} 尝试加锁，失败则包装为 {@link LockAcquireException} 抛出。
     *
     * @param keys      业务锁 key 集合（已拼接前缀）
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 已成功持有的锁句柄
     * @throws LockAcquireException 未获得锁（将失败状态与 key 透传）
     */
    private static LockHandle doAcquire(List<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit) {
        LockAcquireResult lockAcquireResult = tryAcquireInternal(keys, waitTime, leaseTime, timeUnit);
        if (!lockAcquireResult.isSuccess()) {
            throw new LockAcquireException(lockAcquireResult.getStatus(), lockAcquireResult.getLockKey());
        }
        return lockAcquireResult.getLockHandle();
    }

    /**
     * 统一加锁入口：单锁/多锁共用，区分固定租期与看门狗两种模式。
     *
     * @param keys      业务锁 key 集合（已拼接前缀）
     * @param waitTime  抢不到锁时的挂起等待时长（0=立即失败）
     * @param leaseTime 持有租期（<=0 启用看门狗自动续期）
     * @param timeUnit  时间单位
     * @return 加锁结果（成功含句柄，失败仅含状态）
     */
    private static LockAcquireResult tryAcquireInternal(List<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit) {
        String joinedKey = String.join(",", keys);
        int n = keys.size();
        RLock[] arr = new RLock[n];
        for (int i = 0; i < n; i++) {
            arr[i] = client().getLock(keys.get(i));
        }
        RLock combined = (n == 1) ? arr[0] : new RedissonMultiLock(arr);

        boolean locked;
        try {
            if (leaseTime > 0) {
                // 固定租期：到期自动释放，不自动续期（必须保证 leaseTime > 业务最大耗时）
                locked = combined.tryLock(waitTime, leaseTime, timeUnit);
            } else {
                // 看门狗模式：默认租期30s，持锁期间每10s自动续期，进程存活期间不会过期
                locked = combined.tryLock(waitTime, timeUnit);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();  // 恢复中断标记
            log.warn("[RedissonLock] 挂起等待锁时被中断, keys={}", joinedKey);
            return LockAcquireResult.fail(LockStatus.INTERRUPTED, joinedKey);
        }

        if (!locked) {
            // waitTime>0 说明经历了挂起等待 -> TIMEOUT；waitTime=0 立即失败 -> FAILED
            LockStatus st = (waitTime > 0) ? LockStatus.TIMEOUT : LockStatus.FAILED;
            log.warn("[RedissonLock] 获取锁失败[{}], keys={}, waitTime={}{}", st.getDesc(), joinedKey, waitTime, timeUnit);
            return LockAcquireResult.fail(st, joinedKey);
        }

        if (CONFIG.isVerboseLog()) {
            log.info("[RedissonLock] 加锁成功, keys={}, watchdog={}, lease={}ms",
                    joinedKey, leaseTime <= 0, leaseTime > 0 ? timeUnit.toMillis(leaseTime) : -1);
        }
        long leaseMillis = (leaseTime > 0) ? timeUnit.toMillis(leaseTime) : -1L;
        return LockAcquireResult.success(new LockHandle(keys, toList(arr), combined, leaseMillis));
    }

    /**
     * 执行业务：RuntimeException 原样抛出，受检异常包装后抛出（保持堆栈）。
     *
     * @param callback 锁内业务回调
     * @param keys     本次加锁的全部 key（用于异常上下文）
     * @param <T>      业务返回值类型
     * @return 业务返回值
     * @throws RuntimeException 业务抛出的 RuntimeException 原样传播
     */
    private static <T> T invokeBusiness(LockCallback<T> callback, List<String> keys) {
        try {
            return callback.execute();
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("[RedissonLock] 锁内业务执行受检异常, keys=" + keys, e);
        }
    }

    /**
     * 执行业务（可感知锁句柄版）：RuntimeException 原样抛出，受检异常包装为 RuntimeException 后抛出（保持堆栈）。
     *
     * @param callback   可感知锁句柄的锁内业务回调
     * @param lockHandle 已持有的锁句柄（回调内可查 TTL、持有状态等）
     * @param keys       本次加锁的全部 key（用于异常上下文）
     * @param <T>        业务返回值类型
     * @return 业务返回值
     * @throws RuntimeException 业务抛出的 RuntimeException 原样传播
     */
    private static <T> T invokeAwareBusiness(LockAwareCallback<T> callback, LockHandle lockHandle, List<String> keys) {
        try {
            return callback.execute(lockHandle, keys);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException("[RedissonLock] 锁内业务执行受检异常, keys=" + keys, e);
        }
    }

    /**
     * 单锁 key 包装：拼接统一前缀后返回单元素列表，供多锁逻辑复用。
     *
     * @param key 业务锁 key
     * @return 含前缀的单元素 key 列表
     */
    private static List<String> buildSingleKeys(String key) {
        return Collections.singletonList(buildKey(key));
    }

    /**
     * 多锁 key 包装：逐个拼接统一前缀；列表为空抛异常；开启排序时全局有序，降低交叉死锁概率。
     *
     * @param keys 业务锁 key 集合
     * @return 含前缀的 key 列表（可能已排序）
     */
    private static List<String> buildMultiKeys(Collection<String> keys) {
        if (keys == null || keys.isEmpty()) {
            throw new IllegalArgumentException("多锁 key 列表不能为空");
        }
        List<String> built = new ArrayList<>(keys.size());
        for (String k : keys) {
            built.add(buildKey(k));
        }
        if (CONFIG.isSortKeysOnMultiLock()) {
            Collections.sort(built);   // 全局有序加锁，降低交叉死锁概率
        }
        return built;
    }

    /**
     * 拼接统一前缀：最终锁 key = keyPrefix + 业务 key；key 为空或空白抛异常。
     *
     * @param key 业务锁 key
     * @return 含前缀的完整锁 key
     */
    private static String buildKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("锁 key 不能为空");
        }
        return CONFIG.getKeyPrefix() + key;
    }

    /**
     * 数组转列表：将原生 RLock 数组包装为 List，便于上层统一处理。
     *
     * @param arr 原生 RLock 数组
     * @return 等价的 RLock 列表
     */
    private static List<RLock> toList(RLock[] arr) {
        List<RLock> list = new ArrayList<>(arr.length);
        Collections.addAll(list, arr);
        return list;
    }

    /**
     * 计算耗时：当前时间减去起始时间，得到业务持锁执行毫秒数。
     *
     * @param begin 起始时间戳（毫秒）
     * @return 耗时（毫秒）
     */
    private static long cost(long begin) { return System.currentTimeMillis() - begin; }

    /**
     * 慢执行告警：持锁耗时超过阈值时打印告警日志，提示关注锁粒度与租期设置。
     *
     * @param keys  本次加锁的全部 key（用于日志上下文）
     * @param begin 起始时间戳（毫秒）
     */
    private static void warnIfSlow(List<String> keys, long begin) {
        long c = cost(begin);
        if (c > SLOW_COST_WARN_MILLIS) {
            log.warn("[RedissonLock] 持锁执行耗时较长({}ms), 请关注锁粒度与租期设置, keys={}", c, keys);
        }
    }
    /**
     * 加锁 + 执行主业务；锁获取失败时执行 fallback（全自动模式内部复用）.
     *
     * @param keys      已拼接前缀的锁 key 列表
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务回调（锁获取成功时执行）
     * @param fallback  降级回调（锁获取失败时执行；可为 null）
     * @param <T>       返回值类型
     * @return 主业务或 fallback 的返回值
     * @throws LockAcquireException 锁获取失败且无 fallback / fallback 也失败时抛出
     */
    private static <T> T doAcquireOrFallback(List<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                              LockCallback<T> callback, LockCallback<T> fallback) {
        LockAcquireResult result = tryAcquireInternal(keys, waitTime, leaseTime, timeUnit);
        if (result.isSuccess()) {
            long begin = System.currentTimeMillis();
            try {
                return invokeBusiness(callback, keys);
            } finally {
                warnIfSlow(keys, begin);
                result.getLockHandle().unlock();
            }
        }
        // 锁获取失败 —— 有 fallback 则执行，无则直接抛异常
        if (fallback != null) {
            log.info("[RedissonLock] 获取锁失败, 执行 fallback, keys={}", keys);
            try {
                return fallback.execute();
            } catch (RuntimeException re) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), re);
            } catch (Exception e) {
                throw new LockAcquireException(result.getStatus(), result.getLockKey(), e);
            }
        }
        throw new LockAcquireException(result.getStatus(), result.getLockKey());
    }

    /**
     * 加锁 + 执行主业务（免异常版）；锁获取失败时执行 fallback，一切结果封装为 LockResult.
     *
     * @param keys      已拼接前缀的锁 key 列表
     * @param waitTime  等待时长
     * @param leaseTime 租期
     * @param timeUnit  时间单位
     * @param callback  主业务回调（锁获取成功时执行）
     * @param fallback  降级回调（锁获取失败时执行；可为 null）
     * @param <T>       返回值类型
     * @return 统一执行结果（绝不抛异常）
     */
    private static <T> LockResult<T> doAcquireOrFallbackExceptionFree(List<String> keys, long waitTime, long leaseTime, TimeUnit timeUnit,
                                                                       LockCallback<T> callback, LockCallback<T> fallback) {
        LockAcquireResult acquireResult = tryAcquireInternal(keys, waitTime, leaseTime, timeUnit);
        if (acquireResult.isSuccess()) {
            long begin = System.currentTimeMillis();
            try {
                T data = callback.execute();
                return LockResult.success(data, acquireResult.getLockKey(), cost(begin));
            } catch (Exception bizException) {
                log.error("[RedissonLock] 锁内业务执行异常, keys={}", keys, bizException);
                return LockResult.bizError(acquireResult.getLockKey(), cost(begin), bizException);
            } finally {
                acquireResult.getLockHandle().unlock();
            }
        }
        // 锁获取失败 —— 有 fallback 则执行
        if (fallback != null) {
            log.info("[RedissonLock] 获取锁失败, 执行 fallback, keys={}", keys);
            try {
                T data = fallback.execute();
                return LockResult.fallbackSuccess(data, acquireResult.getLockKey(), acquireResult.getStatus());
            } catch (Exception fallbackException) {
                log.error("[RedissonLock] fallback 执行异常, keys={}", keys, fallbackException);
                return LockResult.fallbackError(acquireResult.getLockKey(), acquireResult.getStatus(), fallbackException);
            }
        }
        return LockResult.lockFail(acquireResult.getStatus(), acquireResult.getLockKey());
    }
}
