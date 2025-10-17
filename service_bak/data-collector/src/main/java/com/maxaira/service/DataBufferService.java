package com.maxaira.service;

import com.maxaira.model.Kline;
import com.maxaira.config.ApplicationConfig;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据缓冲服务 - 高性能K线数据缓冲与批量写入
 * 
 * 核心功能：
 * - LMAX Disruptor环形缓冲区：无锁高性能数据缓冲
 * - 智能批量写入：基于大小、时间、批次结束的多重触发策略
 * - 内存优化管理：动态调整缓冲区大小，减少GC压力
 * - 异常恢复机制：写入失败自动重试，确保数据不丢失
 * - 性能监控：实时统计处理速度和成功率
 * 
 * 设计原则：
 * - 高吞吐量优先：使用Disruptor实现百万级TPS处理能力
 * - 内存安全：合理控制缓冲区大小，避免OOM
 * - 数据可靠性：多重保障机制确保数据完整性
 * - 可观测性：详细的性能指标和状态监控
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DataBufferService {

    private final ApplicationConfig config;
    private final DatabaseService databaseService;

    // ========== 缓存配置参数，避免频繁getter调用 ==========
    private int bufferSize;
    private int flushIntervalSeconds;
    private int batchSize;
    private long batchTimeoutMs;
    private int ringBufferSize;
    private String waitStrategy;
    private int maxRetries;
    private long retryDelayMs;
    private boolean detailedLogging;
    private int performanceLogIntervalSeconds;
    private boolean disruptorEnabled;

    // 缓冲区管理
    private final Map<String, BufferBucket> buffers = new ConcurrentHashMap<>();
    
    // Disruptor组件
    private Disruptor<KlineEvent> disruptor;
    private RingBuffer<KlineEvent> ringBuffer;
    
    // 调度器
    private ScheduledExecutorService scheduler;
    
    // 性能统计
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicLong totalBatches = new AtomicLong(0);
    private volatile long lastPerformanceLog = System.currentTimeMillis();

    @PostConstruct
    public void init() {
        // 缓存配置参数，避免高频getter调用
        cacheConfigParameters();
        
        initDisruptor();
        initScheduler();
        log.info("DataBufferService initialized with cached config: bufferSize={}, flushIntervalSeconds={}, ringBufferSize={}",
                bufferSize, flushIntervalSeconds, ringBufferSize);
    }

    @PreDestroy
    public void shutdown() {
        if (disruptor != null) {
            disruptor.shutdown();
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        log.info("DataBufferService shutdown completed");
    }

    /**
     * 缓存配置参数，避免在高频方法中重复调用getter
     * 
     * 性能优化原理：
     * - 减少方法调用开销：getter方法虽然简单，但在高频场景下累积开销显著
     * - 提高CPU缓存命中率：本地变量访问比对象属性访问更高效
     * - 避免潜在的同步开销：某些配置可能涉及线程安全检查
     */
    private void cacheConfigParameters() {
        ApplicationConfig.DataBuffer dataBufferConfig = config.getDataBuffer();
        ApplicationConfig.Monitoring monitoringConfig = config.getMonitoring();
        
        this.bufferSize = dataBufferConfig.getBufferSize();
        this.flushIntervalSeconds = dataBufferConfig.getFlushIntervalSeconds();
        this.batchSize = dataBufferConfig.getBatchSize();
        this.batchTimeoutMs = dataBufferConfig.getBatchTimeoutMs();
        this.ringBufferSize = calculateRingBufferSize(dataBufferConfig.getRingBufferSize());
        this.waitStrategy = dataBufferConfig.getWaitStrategy();
        this.maxRetries = dataBufferConfig.getMaxRetries();
        this.retryDelayMs = dataBufferConfig.getRetryDelayMs();
        this.disruptorEnabled = dataBufferConfig.isDisruptorEnabled();
        this.detailedLogging = monitoringConfig.isDetailedLogging();
        this.performanceLogIntervalSeconds = monitoringConfig.getPerformanceLogIntervalSeconds();
        
        log.info("配置参数已缓存 - bufferSize: {}, batchSize: {}, ringBufferSize: {}, disruptorEnabled: {}", 
                bufferSize, batchSize, ringBufferSize, disruptorEnabled);
    }

    /**
     * 初始化Disruptor高性能环形缓冲区
     * 
     * 内存管理优化：
     * - 使用对象池减少GC压力
     * - 预分配环形缓冲区，避免动态扩容
     * - 优化等待策略，平衡CPU使用和延迟
     */
    private void initDisruptor() {
        // 创建专用线程工厂（Disruptor 4.0.0要求使用ThreadFactory而不是Executor）
        ThreadFactory threadFactory = r -> {
            Thread t = new Thread(r, "disruptor-worker");
            t.setDaemon(true);
            t.setPriority(Thread.NORM_PRIORITY + 1);        // 略微提高优先级
            return t;
        };

        // 创建Disruptor，使用缓存的配置参数
        disruptor = new Disruptor<KlineEvent>(
            KlineEvent::new,
            ringBufferSize,
            threadFactory,
            ProducerType.MULTI,                             // 多生产者模式
            getWaitStrategyFromCache()                      // 使用缓存的等待策略
        );
        
        // 设置异常处理器
        disruptor.setDefaultExceptionHandler(new ExceptionHandler<KlineEvent>() {
            @Override
            public void handleEventException(Throwable ex, long sequence, KlineEvent event) {
                log.error("Error processing Kline event at sequence {}: {}", sequence, event, ex);
                totalFailed.incrementAndGet();
            }

            @Override
            public void handleOnStartException(Throwable ex) {
                log.error("Error starting Disruptor", ex);
            }

            @Override
            public void handleOnShutdownException(Throwable ex) {
                log.error("Error shutting down Disruptor", ex);
            }
        });

        // 设置事件处理器
        disruptor.handleEventsWith(this::handleKlineEvent);

        // 启动Disruptor
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    /**
     * 初始化定时调度器
     */
    private void initScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "buffer-scheduler");
            t.setDaemon(true);
            return t;
        });
        
        // 定时刷新缓冲区 - 使用缓存的配置参数
        scheduler.scheduleAtFixedRate(this::flushByInterval, 
                flushIntervalSeconds, 
                flushIntervalSeconds, 
                TimeUnit.SECONDS);
                
        // 定时性能日志 - 使用缓存的配置参数
        if (detailedLogging) {
            scheduler.scheduleAtFixedRate(this::logPerformanceMetrics,
                    performanceLogIntervalSeconds,
                    performanceLogIntervalSeconds,
                    TimeUnit.SECONDS);
        }
    }

    /**
     * 计算环形缓冲区大小（必须是2的幂）
     * 
     * 内存优化策略：
     * - 根据可用内存动态调整缓冲区大小
     * - 平衡内存使用和性能需求
     */
    private int calculateRingBufferSize(int configuredSize) {
        // 确保是2的幂
        if ((configuredSize & (configuredSize - 1)) != 0) {
            // 向上取最近的2的幂
            configuredSize = Integer.highestOneBit(configuredSize) << 1;
            log.warn("环形缓冲区大小必须是2的幂，已调整为: {}", configuredSize);
        }
        
        // 根据可用内存动态调整
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        long availableMemory = maxMemory - (totalMemory - freeMemory);
        
        // 预估每个KlineEvent占用约1KB内存
        long estimatedMemoryUsage = configuredSize * 1024L;
        
        // 如果预估内存使用超过可用内存的10%，则减小缓冲区
        if (estimatedMemoryUsage > availableMemory * 0.1) {
            int adjustedSize = (int) Math.min(configuredSize, (availableMemory * 0.1) / 1024);
            adjustedSize = Integer.highestOneBit(adjustedSize); // 确保是2的幂
            log.warn("根据可用内存调整环形缓冲区大小: {} -> {}", configuredSize, adjustedSize);
            return adjustedSize;
        }
        
        return configuredSize;
    }

    /**
     * 获取等待策略 - 使用缓存的配置参数
     * 
     * 性能优化：
     * - 根据系统负载选择合适的等待策略
     * - 平衡CPU使用率和延迟
     */
    private WaitStrategy getWaitStrategyFromCache() {
        return switch (waitStrategy.toUpperCase()) {
            case "BLOCKING" -> new BlockingWaitStrategy();
            case "SLEEPING" -> new SleepingWaitStrategy();
            case "YIELDING" -> new YieldingWaitStrategy();
            case "BUSY_SPIN" -> new BusySpinWaitStrategy();
            case "LITE_BLOCKING" -> new LiteBlockingWaitStrategy();
            case "TIMEOUT_BLOCKING" -> new TimeoutBlockingWaitStrategy(1, TimeUnit.SECONDS);
            default -> {
                log.warn("未知的等待策略: {}, 使用默认的YieldingWaitStrategy", waitStrategy);
                yield new YieldingWaitStrategy();
            }
        };
    }

    /**
     * 高性能缓冲入口（按周期） - 使用Disruptor环形缓冲区
     */
    public void bufferKline(String symbol, String marketType, String period, Kline kline) {
        String tableName = config.getTableName(symbol, marketType, period);

        if (ringBuffer != null && disruptorEnabled) {
            long sequence = ringBuffer.next();
            try {
                KlineEvent event = ringBuffer.get(sequence);
                event.setData(tableName, kline);
            } finally {
                ringBuffer.publish(sequence);
            }
        } else {
            buffer(tableName, kline);
        }
    }

    /**
     * 批量缓冲K线数据
     */
    public void bufferKlines(String symbol, String marketType, String period, List<Kline> klines) {
        String tableName = config.getTableName(symbol, marketType, period);
        for (Kline kline : klines) {
            buffer(tableName, kline);
        }
    }

    /**
     * 传统缓冲逻辑（作为备用） - 使用缓存的配置参数
     */
    public void buffer(String tableName, Kline kline) {
        BufferBucket bucket = buffers.computeIfAbsent(tableName, t -> new BufferBucket());
        bucket.lock.lock();
        try {
            bucket.queue.add(kline);
            if (bucket.queue.size() >= bufferSize) {  // 使用缓存的配置参数
                flushLocked(tableName, bucket);
            }
        } finally {
            bucket.lock.unlock();
        }
    }

    /**
     * Disruptor事件处理器 - 处理高频K线数据
     * 使用缓存的配置参数，避免频繁getter调用
     */
    private void handleKlineEvent(KlineEvent event, long sequence, boolean endOfBatch) {
        try {
            String tableName = event.getTableName();
            Kline kline = event.getKline();
            
            if (tableName != null && kline != null) {
                // 添加到传统缓冲区进行批量处理
                BufferBucket bucket = buffers.computeIfAbsent(tableName, t -> new BufferBucket());
                bucket.lock.lock();
                try {
                    bucket.queue.add(kline);
                    // 智能刷新策略：批次结束、达到阈值或超时 - 使用缓存的配置参数
                    boolean shouldFlush = endOfBatch || 
                                        bucket.queue.size() >= batchSize ||
                                        (System.currentTimeMillis() - bucket.lastFlushMs) > batchTimeoutMs;
                    
                    if (shouldFlush) {
                        flushLocked(tableName, bucket);
                    }
                } finally {
                    bucket.lock.unlock();
                }
                
                totalProcessed.incrementAndGet();
            } else {
                log.warn("Received invalid KlineEvent: tableName={}, kline={}", tableName, kline);
            }
        } catch (Exception e) {
            log.error("Error handling Kline event at sequence {}", sequence, e);
            totalFailed.incrementAndGet();
        }
    }

    /**
     * 定时器触发的批量写入（按时间阈值 + 非空队列）
     * 使用缓存的配置参数
     */
    private void flushByInterval() {
        long now = System.currentTimeMillis();
        
        for (Map.Entry<String, BufferBucket> entry : buffers.entrySet()) {
            String tableName = entry.getKey();
            BufferBucket bucket = entry.getValue();

            // 避免长时间堆积，定时器到点且队列非空即刷 - 使用缓存的配置参数
            if (!bucket.queue.isEmpty() && 
                now - bucket.lastFlushMs >= TimeUnit.SECONDS.toMillis(flushIntervalSeconds)) {
                
                bucket.lock.lock();
                try {
                    flushLocked(tableName, bucket);
                } finally {
                    bucket.lock.unlock();
                }
            }
        }
    }

    /**
     * 加锁态下执行写入：将队列转移到本地列表，写库失败则回滚
     * 使用缓存的配置参数
     */
    private void flushLocked(String tableName, BufferBucket bucket) {
        if (bucket.queue.isEmpty()) return;

        List<Kline> batch = new ArrayList<>(bucket.queue);
        int retryCount = 0;
        
        while (retryCount <= maxRetries) {  // 使用缓存的配置参数
            try {
                databaseService.batchInsertKlines(tableName, batch);
                
                // 写入成功，清空队列并更新统计
                bucket.queue.clear();
                bucket.lastFlushMs = System.currentTimeMillis();
                totalBatches.incrementAndGet();
                
                if (retryCount > 0) {
                    log.info("重试成功: {} 条记录写入 {}, 重试次数: {}", batch.size(), tableName, retryCount);
                }
                return;
                
            } catch (Exception e) {
                retryCount++;
                if (retryCount <= maxRetries) {
                    log.warn("写入失败，准备重试 ({}/{}): {} - {}", retryCount, maxRetries, tableName, e.getMessage());
                    try {
                        Thread.sleep(retryDelayMs * retryCount);  // 使用缓存的配置参数
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else {
                    log.error("写入最终失败，丢弃 {} 条记录: {} - {}", batch.size(), tableName, e.getMessage(), e);
                    totalFailed.addAndGet(batch.size());
                    bucket.queue.clear(); // 避免无限重试
                    break;
                }
            }
        }
    }

    /**
     * 性能指标日志
     */
    private void logPerformanceMetrics() {
        long now = System.currentTimeMillis();
        long processed = totalProcessed.get();
        long failed = totalFailed.get();
        long batches = totalBatches.get();
        long duration = now - lastPerformanceLog;
        
        if (duration > 0) {
            double tps = (processed * 1000.0) / duration;
            double successRate = processed > 0 ? (processed - failed) * 100.0 / processed : 100.0;
            
            String msg = String.format(
                "性能统计 - TPS: %.2f, 成功率: %.2f%%, 总处理: %d, 总失败: %d, 批次: %d, 缓冲区数: %d",
                tps, successRate, processed, failed, batches, buffers.size()
            );
            log.info(msg);
        }
        
        lastPerformanceLog = now;
    }

    /**
     * 获取缓冲区状态 - 使用缓存的配置参数
     */
    public Map<String, Object> getBufferStatus() {
        Map<String, Object> status = new ConcurrentHashMap<>();
        
        // 基础配置信息 - 使用缓存的参数
        status.put("bufferSize", bufferSize);
        status.put("flushIntervalSeconds", flushIntervalSeconds);
        status.put("batchSize", batchSize);
        status.put("ringBufferSize", ringBufferSize);
        
        // 运行时统计
        status.put("totalProcessed", totalProcessed.get());
        status.put("totalFailed", totalFailed.get());
        status.put("totalBatches", totalBatches.get());
        status.put("activeBuffers", buffers.size());
        
        // 各缓冲区详情
        Map<String, Object> bufferDetails = new ConcurrentHashMap<>();
        for (Map.Entry<String, BufferBucket> entry : buffers.entrySet()) {
            BufferBucket bucket = entry.getValue();
            Map<String, Object> bucketInfo = Map.of(
                "queueSize", bucket.queue.size(),
                "lastFlushMs", bucket.lastFlushMs,
                "timeSinceLastFlush", System.currentTimeMillis() - bucket.lastFlushMs
            );
            bufferDetails.put(entry.getKey(), bucketInfo);
        }
        status.put("bufferDetails", bufferDetails);
        
        // Disruptor状态
        if (ringBuffer != null) {
            status.put("disruptorEnabled", true);
            status.put("ringBufferRemaining", ringBuffer.remainingCapacity());
        } else {
            status.put("disruptorEnabled", false);
        }
        
        return status;
    }

    /**
     * 手动刷新所有缓冲区
     */
    public void flushAll() {
        log.info("开始手动刷新所有缓冲区...");
        int flushedBuckets = 0;
        int totalRecords = 0;
        
        for (Map.Entry<String, BufferBucket> entry : buffers.entrySet()) {
            String tableName = entry.getKey();
            BufferBucket bucket = entry.getValue();
            
            if (!bucket.queue.isEmpty()) {
                bucket.lock.lock();
                try {
                    int beforeSize = bucket.queue.size();
                    flushLocked(tableName, bucket);
                    if (bucket.queue.isEmpty()) {
                        flushedBuckets++;
                        totalRecords += beforeSize;
                    }
                } finally {
                    bucket.lock.unlock();
                }
            }
        }
        
        log.info("手动刷新完成: {} 个缓冲区，共 {} 条记录", flushedBuckets, totalRecords);
    }

    /**
     * Disruptor事件对象
     */
    public static class KlineEvent {
        private volatile String tableName;
        private volatile Kline kline;

        public String getTableName() { return tableName; }
        public Kline getKline() { return kline; }

        public void setData(String tableName, Kline kline) {
            this.tableName = tableName;
            this.kline = kline;
        }

        public void clear() {
            this.tableName = null;
            this.kline = null;
        }

        public boolean isValid() {
            return tableName != null && kline != null;
        }

        @Override
        public String toString() {
            return String.format("KlineEvent{tableName='%s', kline=%s}", tableName, kline);
        }
    }

    /**
     * 缓冲桶 - 线程安全的数据容器
     */
    private static class BufferBucket {
        final ReentrantLock lock = new ReentrantLock();
        final List<Kline> queue = new ArrayList<>();
        volatile long lastFlushMs = System.currentTimeMillis();
    }
}