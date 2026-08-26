package org.myc.demo;

import org.myc.demo.distributed.DistributedLockUtils;
import org.redisson.api.RedissonClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Spring Boot 启动类.
 *
 * <p>RedissonClient 由 {@link org.myc.demo.config.RedissonConfig} 手动注册，
 * 此处注册 {@link DistributedLockUtils} 为 Bean 完成静态初始化.
 */
@SpringBootApplication
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    /**
     * 注册 DistributedLockUtils 为 Spring Bean.
     * <p>构造器接收自动装配的 RedissonClient，内部完成静态初始化。
     * 初始化后可直接以静态方式调用 DistributedLockUtils 的所有 API.
     */
    @Bean
    public DistributedLockUtils distributedLockUtils(RedissonClient redissonClient) {
        return new DistributedLockUtils(redissonClient);
    }
}
