package com.maxaira.service;

import com.maxaira.config.ApplicationConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
public class BackfillTaskManager {

    private final ApplicationConfig config;
    private final BackfillService backfillService;
    private final ConcurrentMap<String, Boolean> enqueuedLocks = new ConcurrentHashMap<>();
    private final ThreadPoolTaskExecutor executor;

    public BackfillTaskManager(
            ApplicationConfig config,
            @Qualifier("backfillExecutor")
            ThreadPoolTaskExecutor executor,
            BackfillService backfillService) {
        this.config = config;
        this.executor = executor;
        this.backfillService = backfillService;
    }

    public void submitInterval(String symbol, String marketType, String period) {
        String table = config.getTableName(symbol, marketType, period);
        if (enqueuedLocks.putIfAbsent(table, Boolean.TRUE) != null) {
            log.info("队列中已存在任务，跳过入队：{}", table);
            return;
        }
        executor.submit(() -> {
            try {
                // 使用返回的 Future，在全部缺口完成后再释放 enqueuedLocks
                CompletableFuture<Void> f = backfillService.backfillInterval(symbol, marketType, period);
                f.whenComplete((v, ex) -> {
                    enqueuedLocks.remove(table);
                    if (ex != null) {
                        log.warn("缺口并发任务总体失败：{}，table={}", ex.getMessage(), table);
                    }
                });
            } catch (Exception e) {
                log.error("submitInterval 提交失败：{} {} {}", symbol, marketType, period, e);
                enqueuedLocks.remove(table);
            }
        });
    }
}