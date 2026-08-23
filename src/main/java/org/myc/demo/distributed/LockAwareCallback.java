package org.myc.demo.distributed;

import java.util.List;
import java.util.concurrent.locks.Lock;

/**
 * 可感知锁信息的业务回调接口（全自动模式使用）.
 * <p>相比 {@link LockCallback}，本回调在执行业务时可拿到 {@link LockHandle}，
 * 用于查询剩余 TTL、判断持有状态等；注意不要在回调内手动 unlock（框架会统一释放）.
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
