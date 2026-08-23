package org.myc.demo.distributed;

/**
 * 锁内业务回调接口（全自动模式使用）.
 * <p>在持有分布式锁期间执行业务逻辑，业务异常会原样上抛（受检异常包装为 RuntimeException）；
 * 与 {@link LockAwareCallback} 的区别是不能在业务内感知锁状态.
 */
@FunctionalInterface
public interface LockCallback<T> {

    /**
     * 在持有分布式锁期间执行的业务逻辑.
     * <p>业务返回值即锁方法的返回值；业务抛异常时，全自动模式原样上抛（受检异常包装为 RuntimeException），
     * 免异常模式（tryExecuteWith*）则被捕获并封装进 {@link LockResult#getError()}。
     *
     * @return 业务执行结果
     * @throws Exception 业务异常，全自动模式向上抛出；免异常模式被捕获进 LockResult
     */
    T execute() throws Exception;
}
