package com.maxaira.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class SchedulingConfig {
    
    @Bean
    public ThreadPoolTaskScheduler taskScheduler(ApplicationConfig config) {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(config.getPerformance().getSchedulePoolSize());
        scheduler.setThreadNamePrefix("app-scheduler-");
        scheduler.initialize();
        return scheduler;
    }
}