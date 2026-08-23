package org.myc.demo.distributed;

/**
 * 分布式锁获取失败异常.
 * <p>在「全自动模式」业务未拿到锁、或「半自动-抛异常版」获取锁失败时抛出，
 * 携带 {@link LockStatus} 失败状态与锁 key，便于定位排查.
 */
public class LockAcquireException extends RuntimeException {

    private final LockStatus status;
    private final String lockKey;

    public LockAcquireException(LockStatus status, String lockKey) {
        super("获取分布式锁失败[" + status.getDesc() + "], key=" + lockKey);
        this.status = status;
        this.lockKey = lockKey;
    }

    public LockAcquireException(LockStatus status, String lockKey, Throwable cause) {
        super("获取分布式锁失败[" + status.getDesc() + "], key=" + lockKey, cause);
        this.status = status;
        this.lockKey = lockKey;
    }

    /**
     * 获取加锁失败的状态（如 FAILED / TIMEOUT / INTERRUPTED），即本次异常的原因。
     *
     * @return 失败状态
     */
    public LockStatus getStatus() {
        return status;
    }

    /**
     * 获取加锁失败的 key，便于错误定位与日志排查。
     *
     * @return 失败的锁 key
     */
    public String getLockKey() {
        return lockKey;
    }
}
