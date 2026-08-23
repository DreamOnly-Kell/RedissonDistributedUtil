package org.myc.demo.distributed;

/**
 * 分布式锁获取状态枚举.
 * <p>包含四种取值：{@code SUCCESS}（加锁成功）、{@code FAILED}（未进入等待即失败）、
 * {@code TIMEOUT}（挂起等待超时未获得锁）、{@code INTERRUPTED}（等待中被中断）；
 * 通过 {@link #isAcquired()} 判断是否已成功获取锁.
 */
public enum LockStatus {
    /**
     * 加锁成功
     */
    SUCCESS(true, "加锁成功"),
    /**
     * 加锁失败：未进入等待即失败（如 waitTime=0 时锁被占用），适合“抢不到就跳过”场景
     */
    FAILED(false, "加锁失败"),
    /**
     * 挂起超时：线程挂起等待了 waitTime 后仍然未获得锁
     */
    TIMEOUT(false, "挂起等待超时未获得锁"),
    /**
     * 挂起等待过程中线程被中断
     */
    INTERRUPTED(false, "挂起等待中被中断"),
    ;

    /**
     * 该状态是否表示已成功获取锁。
     */
    private final boolean acquired;
    /**
     * 状态的中文描述（用于日志与错误信息）。
     */
    private final String desc;

    LockStatus(boolean acquired, String desc) {
        this.acquired = acquired;
        this.desc = desc;
    }

    /**
     * 该状态是否表示已成功获取锁.
     *
     * @return 已成功获取锁时为 true
     */
    public boolean isAcquired() {
        return acquired;
    }

    /**
     * 返回状态的中文描述.
     *
     * @return 状态描述
     */
    public String getDesc() {
        return desc;
    }
}
