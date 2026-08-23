package org.myc.demo.distributed;

import java.util.concurrent.locks.Lock;

/**
 * 半自动获取锁结果的载体.
 * <p>封装一次分布式锁获取尝试的结果：成功时携带 {@link LockHandle}，失败时携带 {@link LockStatus}。
 * 通过 {@link #isSuccess()} 判断结果状态，或通过 {@link #requireHandle()} 在前置条件满足时直接获取 handle。
 */
public class LockAcquireResult {
    private final LockStatus status;
    private final LockHandle lockHandle;
    private final String lockKey;

    /**
     * 构造锁获取结果.
     *
     * @param status    锁获取状态，不能为 null
     * @param handle    锁句柄，成功时不能为 null
     * @param lockKey   锁的 key，不能为 null
     */
    public LockAcquireResult(LockStatus status, LockHandle handle, String lockKey) {
        this.status = status;
        this.lockHandle = handle;
        this.lockKey = lockKey;
    }

    /**
     * 成功.
     *
     * @param handle 已成功获取的锁handle，不能为 null
     * @return 成功状态的锁获取结果
     */
    public static LockAcquireResult success(LockHandle handle) {
        return new LockAcquireResult(LockStatus.SUCCESS, handle, String.join(",", handle.getKeys()));
    }

    /**
     * 失败.
     *
     * @param status  失败的锁状态，必须为非成功状态
     * @param lockKey 锁的 key
     * @return 失败状态的锁获取结果，其 handle 为 null
     */
    public static LockAcquireResult fail(LockStatus status, String lockKey) {
        return new LockAcquireResult(status, null, lockKey);
    }

    /**
     * 判断锁是否成功获取.
     *
     * @return {@code true} 表示锁已成功获取，{@code false} 表示获取失败
     */
    public boolean isSuccess() {
        return status.isAcquired();
    }

    /**
     * 返回锁获取状态.
     * <p>即本次获取尝试的结果分类（SUCCESS / FAILED / TIMEOUT / INTERRUPTED）。
     *
     * @return 锁状态
     */
    public LockStatus getStatus() {
        return status;
    }

    /**
     * 返回锁handle，可能为空（当获取失败时为 null）.
     * <p>成功时返回实际持有的句柄，失败时返回 {@code null}。
     *
     * @return 锁句柄，失败时为 null
     */
    public LockHandle getLockHandle() {
        return lockHandle;
    }

    /**
     * 返回锁的 key.
     * @return 锁的 key
     */
    public String getLockKey() {
        return lockKey;
    }

    /**
     * 获取锁handle，调用前必须已成功获取锁.
     * <p>前置条件：{@link #isSuccess()} 必须为 {@code true}，否则抛出 IllegalStateException。
     *
     * @return 锁句柄，不会为 null
     * @throws IllegalStateException 当 {@link #isSuccess()} 为 {@code false} 时抛出，异常信息包含锁状态和锁 key
     */
    public LockHandle requireHandle() {
        if (!isSuccess()) {
            throw new IllegalStateException("获取锁失败[" + status.getDesc() + "], key=" + lockKey);
        }
        return lockHandle;
    }

    /**
     * 返回调试友好的字符串表示，包含加锁状态与锁 key。
     *
     * @return 形如 LockAcquireResult{status=..., lockKey='...'} 的字符串
     */
    @Override
    public String toString() {
        return "LockAcquireResult{status=" + status + ", lockKey='" + lockKey + "'}";
    }
}
