package org.myc.demo.distributed;

import java.io.Serializable;

/**
 * 免异常模式的统一执行结果（任何情况下都不会向外抛出异常）。
 *
 * <pre>
 * 五种典型结果：
 * 1. lockStatus = SUCCESS 且 bizSuccess = true    -> 拿到锁且业务成功，data 为业务返回值
 * 2. lockStatus = SUCCESS 且 bizSuccess = false   -> 拿到锁但业务抛异常，见 error
 * 3. lockStatus = FAILED / TIMEOUT / INTERRUPTED  -> 未获取到锁（立即失败 / 挂起超时 / 被中断），业务未执行
 * 4. lockStatus = FAILED/TIMEOUT/INTERRUPTED 且 bizSuccess = true  -> 锁未拿到但 fallback 降级成功，data 为 fallback 返回值
 * 5. lockStatus = FAILED/TIMEOUT/INTERRUPTED 且 bizSuccess = false 且 error != null -> 锁未拿到且 fallback 也失败
 * </pre>
 *
 * @param <T> 业务返回值的数据类型
 */
public class LockResult<T> implements Serializable {

    /**
     * 锁获取状态.
     */
    private final LockStatus lockStatus;
    /**
     * 业务是否执行成功（仅lockStatus=SUCCESS 时有效）
     */
    private final boolean bizSuccess;
    /**
     * 业务返回值.
     */
    private final T data;
    /**
     * 业务执行或解锁过程中的异常.
     */
    private final Throwable error;
    /**
     * 锁 key（多锁时逗号分隔）
     */
    private final String lockKey;
    /**
     * 持锁执行耗时（毫秒）
     */
    private final long costMillis;

    private LockResult(LockStatus lockStatus, boolean bizSuccess, T data, Throwable error, String lockKey, long costMillis) {
        this.lockStatus = lockStatus;
        this.bizSuccess = bizSuccess;
        this.data = data;
        this.error = error;
        this.lockKey = lockKey;
        this.costMillis = costMillis;
    }

    /**
     * 成功
     * @param data 业务返回值
     * @param lockKey 锁 key
     * @param costMillis 总耗时
     * @return 成功状态的 LockResult
     */
    public static <T> LockResult<T> success(T data, String lockKey, long costMillis) {
        return new LockResult<>(LockStatus.SUCCESS, true, data, null, lockKey, costMillis);
    }

    /**
     * 业务错误（已拿到锁但业务抛异常）.
     * @param lockKey 锁 key
     * @param costMillis 总耗时
     * @param error 业务异常
     * @return 业务失败状态的 LockResult（lockStatus=SUCCESS, bizSuccess=false）
     */
    public static <T> LockResult<T> bizError(String lockKey, long costMillis, Throwable error) {
        return new LockResult<>(LockStatus.SUCCESS, false, null, error, lockKey, costMillis);
    }

    /**
     * 锁失败（未拿到锁，业务未执行）.
     * @param status 锁获取失败状态（FAILED / TIMEOUT / INTERRUPTED）
     * @param lockKey 锁 key
     * @return 锁失败状态的 LockResult（handle 为 null）
     */
    public static <T> LockResult<T> lockFail(LockStatus status, String lockKey) {
        return new LockResult<>(status, false, null, null, lockKey, 0L);
    }
    /**
     * fallback 成功（未拿到锁，但 fallback 降级执行成功）.
     * <p>lockStatus 保持失败状态（锁确实没拿到），bizSuccess=true，data 为 fallback 返回值.
     *
     * @param data     fallback 返回值
     * @param lockKey  锁 key
     * @param status   锁获取失败状态（FAILED / TIMEOUT / INTERRUPTED）
     * @return fallback 成功状态的 LockResult
     */
    public static <T> LockResult<T> fallbackSuccess(T data, String lockKey, LockStatus status) {
        return new LockResult<>(status, true, data, null, lockKey, 0L);
    }

    /**
     * fallback 执行异常（未拿到锁，fallback 也失败了）.
     * <p>lockStatus 保持失败状态，bizSuccess=false，error 为 fallback 异常.
     *
     * @param lockKey  锁 key
     * @param status   锁获取失败状态（FAILED / TIMEOUT / INTERRUPTED）
     * @param error    fallback 执行异常
     * @return fallback 失败状态的 LockResult
     */
    public static <T> LockResult<T> fallbackError(String lockKey, LockStatus status, Throwable error) {
        return new LockResult<>(status, false, null, error, lockKey, 0L);
    }

    /**
     * 快捷判断：是否拿到锁且业务执行成功.
     *
     * @return 拿到锁且业务成功时为 true
     */
    public boolean isOk() {
        return lockStatus.isAcquired() && bizSuccess;
    }

    /**
     * 获取锁状态.
     *
     * @return 锁获取状态（SUCCESS / FAILED / TIMEOUT / INTERRUPTED）
     */
    public LockStatus getLockStatus() {
        return lockStatus;
    }

    /**
     * 业务是否执行成功.
     * <p>lockStatus=SUCCESS 时表示主业务成功；lockStatus!=SUCCESS 时表示 fallback 降级成功.
     *
     * @return 业务成功时为 true
     */
    public boolean isBizSuccess() {
        return bizSuccess;
    }

    /**
     * 获取业务返回值（业务失败时可能为 null）.
     *
     * @return 业务返回值
     */
    public T getData() {
        return data;
    }

    /**
     * 获取业务/解锁过程中的异常（无异常时为 null）.
     *
     * @return 异常对象；无异常时为 null
     */
    public Throwable getError() {
        return error;
    }

    /**
     * 获取锁 key（多锁时逗号分隔）.
     *
     * @return 锁 key
     */
    public String getLockKey() {
        return lockKey;
    }

    /**
     * 获取持锁执行总耗时（毫秒）.
     *
     * @return 耗时（毫秒）
     */
    public long getCostMillis() {
        return costMillis;
    }

    /**
     * 返回调试友好的字符串表示，包含锁状态、业务成败、返回值、异常、锁 key 与耗时。
     *
     * @return 形如 LockResult{lockStatus=..., bizSuccess=..., ...} 的字符串
     */
    @Override
    public String toString() {
        return "LockResult{lockStatus=" + lockStatus + ", bizSuccess=" + bizSuccess
                + ", data=" + data + ", error=" + (error == null ? null : error.toString())
                + ", lockKey='" + lockKey + "', costMillis=" + costMillis + "}";
    }
}
