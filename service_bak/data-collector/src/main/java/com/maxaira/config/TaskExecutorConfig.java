package com.maxaira.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class TaskExecutorConfig {

    @Bean("backfillExecutor")
    public ThreadPoolTaskExecutor backfillExecutor(ApplicationConfig config) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(config.getPerformance().getTaskCoreSize());
        exec.setMaxPoolSize(config.getPerformance().getTaskMaxSize());
        exec.setQueueCapacity(config.getPerformance().getTaskQueueCapacity());
        exec.setThreadNamePrefix("backfill-");
        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        exec.initialize();
        return exec;
    }

    @Bean("windowExecutor")
    public ThreadPoolTaskExecutor windowExecutor(ApplicationConfig config) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(Math.min(16, Math.max(2, config.getPerformance().getTaskCoreSize())));
        exec.setMaxPoolSize(config.getPerformance().getTaskMaxSize());
        exec.setQueueCapacity(config.getPerformance().getTaskQueueCapacity());
        exec.setThreadNamePrefix("window-");
        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        exec.initialize();
        return exec;
    }

    @Bean("writeExecutor")
    public ThreadPoolTaskExecutor writeExecutor(ApplicationConfig config) {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(Math.min(8, Math.max(2, config.getPerformance().getTaskCoreSize())));
        exec.setMaxPoolSize(Math.min(16, config.getPerformance().getTaskMaxSize()));
        exec.setQueueCapacity(config.getPerformance().getTaskQueueCapacity());
        exec.setThreadNamePrefix("write-");
        exec.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        exec.initialize();
        return exec;
    }
}