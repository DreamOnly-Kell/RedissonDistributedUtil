package org.myc.demo.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redisson 手动配置类.
 *
 * <p>从 application.yml 读取 spring.redis.redisson.* 配置，
 * 构建并注册 {@link RedissonClient} Bean.
 *
 * <pre>
 * spring:
 *   redis:
 *     redisson:
 *       address: redis://127.0.0.1:6379
 *       password: (可选)
 *       database: 0
 *       connect-timeout: 5000
 *       timeout: 3000
 *       pool-size: 64
 *       idle-size: 10
 * </pre>
 */
@Configuration
public class RedissonConfig {

    @Bean
    @ConfigurationProperties(prefix = "spring.redis.redisson")
    public RedissonProperties redissonProperties() {
        return new RedissonProperties();
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient(RedissonProperties props) {
        Config config = new Config();
        var singleServer = config.useSingleServer()
                .setAddress(props.getAddress())
                .setDatabase(props.getDatabase())
                .setConnectTimeout(props.getConnectTimeout())
                .setTimeout(props.getTimeout())
                .setConnectionPoolSize(props.getPoolSize())
                .setConnectionMinimumIdleSize(props.getIdleSize());
        if (props.getPassword() != null && !props.getPassword().isEmpty()) {
            singleServer.setPassword(props.getPassword());
        }
        return Redisson.create(config);
    }

    /**
     * Redisson 配置属性（绑定 application.yml 中 spring.redis.redisson.*）.
     */
    public static class RedissonProperties {
        private String address = "redis://127.0.0.1:6379";
        private String password;
        private int database = 0;
        private int connectTimeout = 5000;
        private int timeout = 3000;
        private int poolSize = 64;
        private int idleSize = 10;

        public String getAddress() { return address; }
        public void setAddress(String address) { this.address = address; }

        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }

        public int getDatabase() { return database; }
        public void setDatabase(int database) { this.database = database; }

        public int getConnectTimeout() { return connectTimeout; }
        public void setConnectTimeout(int connectTimeout) { this.connectTimeout = connectTimeout; }

        public int getTimeout() { return timeout; }
        public void setTimeout(int timeout) { this.timeout = timeout; }

        public int getPoolSize() { return poolSize; }
        public void setPoolSize(int poolSize) { this.poolSize = poolSize; }

        public int getIdleSize() { return idleSize; }
        public void setIdleSize(int idleSize) { this.idleSize = idleSize; }
    }
}
