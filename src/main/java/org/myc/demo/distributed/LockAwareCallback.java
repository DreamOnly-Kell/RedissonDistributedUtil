package org.myc.demo.distributed;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * 可感知锁信息的回调：可在锁内查询剩余 TTL、判断持有状态等
 */
@FunctionalInterface
public interface LockAwareCallback<T> {
    /**
     * 在持有分布式锁期间执行的业务逻辑.
     * @param handle 锁句柄（可查 TTL、持有锁状态等；注意不要在回调内手动 unlock）
     * @param keys 本次加锁的全部 key（已含统一前缀；单锁时 size=1，多锁时为排序后的完整列表）
     * @throws Exception 业务异常：全自动模式向上抛出（受检异常会被包装为 RuntimeException）
     */
    T execute(LockHandle handle, List<String> keys) throws Exception;
}
