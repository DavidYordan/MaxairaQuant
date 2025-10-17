package com.maxaira;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * Maxaira Data Collector 应用主类
 * 
 * 重构后的新架构入口点，采用清晰的包结构和分层设计
 * 
 * 功能特性：
 * - 启用异步处理：支持WebSocket和批量下载的并发操作
 * - 启用定时任务：支持智能补齐调度
 * - 组件扫描：自动发现所有配置和服务组件
 * 
 * @author Maxaira Team
 * @version 2.0
 */
@SpringBootApplication(scanBasePackages = "com.maxaira")
@EnableScheduling
@EnableAsync
public class DataCollectorApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(DataCollectorApplication.class, args);
    }
}