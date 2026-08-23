package org.myc.demo.distributed;

/**
 * 锁内业务回调（全自动模式使用）
 */
@FunctionalInterface
public interface LockCallback<T> {

    /**
     * 在持有分布式锁期间执行的业务逻辑
     * @return 业务执行结果.
     * @throws Exception 业务异常，全自动模式向上抛出；免异常模式被捕获进LockResult
     */
    T execute() throws Exception;
}
