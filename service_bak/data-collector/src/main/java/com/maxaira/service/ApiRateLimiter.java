package com.maxaira.service;

import com.maxaira.config.ApplicationConfig;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class ApiRateLimiter {

    private final ScheduledExecutorService refill = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "api-rate-refill");
        t.setDaemon(true);
        return t;
    });

    private final Semaphore permits;
    private volatile int permitsPerSecond;
    private volatile int burst;
    private volatile long blockUntilMs;
    private volatile long minuteStartMs;
    private volatile int usedWeight1m;
    private static final int SAFETY_MARGIN = 50;

    public ApiRateLimiter(ApplicationConfig config) {
        // 用全局配置“每秒请求数”和“分钟权重上限”
        this.permitsPerSecond = Math.max(1, config.getRequestsPerSecond());
        this.burst = Math.max(1, this.permitsPerSecond * 2);
        this.permits = new Semaphore(burst); // 初始充满
        this.minuteStartMs = alignToMinute(System.currentTimeMillis());
    }

    @PostConstruct
    public void start() {
        refill.scheduleAtFixedRate(() -> {
            int need = permitsPerSecond - permits.availablePermits();
            if (need > 0) {
                permits.release(need);
            }
            // 每分钟窗口刷新
            if (System.currentTimeMillis() - minuteStartMs >= 60_000L) {
                minuteStartMs = alignToMinute(System.currentTimeMillis());
                usedWeight1m = 0;
                blockUntilMs = 0;
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    public void acquire() throws InterruptedException {
        long now = System.currentTimeMillis();
        long pause = blockUntilMs - now;
        if (pause > 0) {
            long sleepTime = Math.min(pause, 1000L);
            Thread.sleep(sleepTime);
        }
        
        // 使用带超时的acquire，避免无限期阻塞
        boolean acquired = permits.tryAcquire(10, TimeUnit.SECONDS);
        if (!acquired) {
            log.error("API速率限制获取许可超时10秒！当前可用许可: {}, 总许可: {}, 线程: {}", 
                    permits.availablePermits(), burst, Thread.currentThread().getName());
            
            // 检查是否有死锁或其他问题
            log.error("线程池可能存在问题，尝试强制获取许可...");
            
            // 再次尝试，但设置更短的超时
            acquired = permits.tryAcquire(2, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("二次尝试仍然失败，跳过此次请求以避免死锁");
                throw new RuntimeException("API速率限制获取许可失败，可能存在死锁");
            }
        }
    }

    // 在每次响应后调用，消化权重与429
    public void afterResponse(okhttp3.Response resp) {
        try {
            String v1m = resp.header("X-MBX-USED-WEIGHT-1M");
            if (v1m != null) {
                usedWeight1m = Integer.parseInt(v1m);
                int remaining = permitsPerSecond * 60 - usedWeight1m - SAFETY_MARGIN;
                if (remaining <= 0) {
                    // 阻塞到下一分钟开始
                    blockUntilMs = alignToMinute(System.currentTimeMillis()) + 60_000L;
                }
            }
            if (resp.code() == 429) {
                String retry = resp.header("Retry-After");
                long retryMs = retry != null ? Long.parseLong(retry) * 1000L : 1000L;
                blockUntilMs = System.currentTimeMillis() + retryMs;
            }
        } catch (Exception ignored) {}
    }

    private long alignToMinute(long t) {
        return (t / 60_000L) * 60_000L;
    }
}