package com.maxaira.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * 应用配置类 - 集中管理配置参数
 * 
 * 配置分层策略：
 * 1. application.yml: 核心业务参数（数据源、缓冲、监控等关键配置）
 * 2. ApplicationConfig: 技术参数、默认值、工具方法
 * 3. Service类: 缓存常用配置值，避免频繁getter调用
 */
@Slf4j
@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class ApplicationConfig {
    
    // ========== 核心业务配置（从yml读取） ==========
    private Binance binance = new Binance();
    private Clickhouse clickhouse = new Clickhouse();
    private Database database = new Database();
    private DataBuffer dataBuffer = new DataBuffer();
    private Websocket websocket = new Websocket();
    private Monitoring monitoring = new Monitoring();
    
    // ========== 技术参数配置（类内默认值） ==========
    private ConnectionPool connectionPool = new ConnectionPool();
    private Performance performance = new Performance();
    private Tables tables = new Tables();
    
    @Data
    public static class Binance {
        private String spotWsUrl;
        private String umWsUrl;
        private String cmWsUrl;
        private String spotRestUrl;
        private String umRestUrl;
        private String cmRestUrl;
        private int apiMaxLimit;
        private int requestsPerSecond;
        private String dataDownloadBaseUrl;
    }
    
    @Data
    public static class Clickhouse {
        private String host;
        private int port;
        private String database;
        private String username;
        private String password;
    }
    
    @Data
    public static class Database {
        private List<String> mainStreamSymbols;
        private List<String> marketTypes;
        private String backfillStartDate;
    }
    
    @Data
    public static class DataBuffer {
        private int bufferSize;
        private int flushIntervalSeconds;
        private int batchSize;
        private long batchTimeoutMs;
        private int ringBufferSize;
        private String waitStrategy;
        
        // 技术参数（默认值）
        private int maxRetries = 3;
        private long retryDelayMs = 1000;
        private boolean disruptorEnabled = true;
        private int producerThreads = 4;
        private int consumerThreads = 2;
        private int maxBatchRetries = 3;
        private boolean enableBatchCompression = true;
    }
    
    @Data
    public static class Websocket {
        private int maxReconnectAttempts;
        private long reconnectDelay;
        private long heartbeatTimeout;
    }
    
    @Data
    public static class Monitoring {
        private boolean metricsEnabled;
        private boolean detailedLogging;
        private int performanceLogIntervalSeconds;
        private int healthCheckIntervalSeconds;
    }
    
    // ========== 技术参数配置类 ==========
    @Data
    public static class ConnectionPool {
        private int maxConnections = 50;
        private int minConnections = 10;
        private int connectionTimeout = 30000;
        private int socketTimeout = 300000;
        private int maxIdleTime = 600000;
        private int validationTimeout = 5000;
        private String validationQuery = "SELECT 1";
        private boolean testOnBorrow = true;
        private boolean testOnReturn = false;
        private boolean testWhileIdle = true;
        private int timeBetweenEvictionRuns = 30000;
    }
    
    @Data
    public static class Performance {
        private int taskCoreSize = 8;
        private int taskMaxSize = 32;
        private int taskQueueCapacity = 1000;
        private int schedulePoolSize = 4;
    }
    
    @Data
    public static class Tables {
        private final Set<String> supportedMarketTypes = Set.of("spot", "um", "cm");
    }
    
    // ========== Getter方法 ==========
    public Binance getBinance() { return binance; }
    public Clickhouse getClickhouse() { return clickhouse; }
    public Database getDatabase() { return database; }
    public DataBuffer getDataBuffer() { return dataBuffer; }
    public Websocket getWebsocket() { return websocket; }
    public Monitoring getMonitoring() { return monitoring; }
    
    // ========== 便捷方法 ==========
    
    /**
     * 获取WebSocket基础URL
     */
    public String getWebSocketBaseUrl(String marketType) {
        return switch (marketType.toLowerCase()) {
            case "spot" -> binance.getSpotWsUrl();
            case "um" -> binance.getUmWsUrl();
            case "cm" -> binance.getCmWsUrl();
            default -> throw new IllegalArgumentException("不支持的市场类型: " + marketType);
        };
    }
    
    /**
     * 构建K线订阅URL
     */
    public String buildKlineUrl(String symbol, String marketType, String period) {
        String baseUrl = getWebSocketBaseUrl(marketType);
        String stream = symbol.toLowerCase() + "@kline_" + period.toLowerCase();
        return baseUrl + stream;
    }

    public String getRestApiBaseUrl(String marketType) {
        return switch (marketType.toLowerCase()) {
            case "spot" -> binance.getSpotRestUrl();
            case "um" -> binance.getUmRestUrl();
            case "cm" -> binance.getCmRestUrl();
            default -> throw new IllegalArgumentException("不支持的市场类型: " + marketType);
        };
    }
    
    /**
     * 生成表名
     */
    public String getTableName(String symbol, String marketType, String period) {
        return String.format("kline_%s_%s_%s", symbol.toLowerCase(), marketType.toLowerCase(), period.toLowerCase());
    }
    
    /**
     * 验证市场类型是否支持
     */
    public boolean isValidMarketType(String marketType) {
        return tables.getSupportedMarketTypes().contains(marketType.toLowerCase());
    }
    
    /**
     * 获取心跳超时时间
     */
    public long getHeartbeatTimeout() {
        return websocket.getHeartbeatTimeout();
    }
    
    /**
     * 获取重连延迟时间
     */
    public long getReconnectDelay() {
        return websocket.getReconnectDelay();
    }
    
    /**
     * 获取最大重连尝试次数
     */
    public int getMaxReconnectAttempts() {
        return websocket.getMaxReconnectAttempts();
    }
    
    /**
     * 获取速率限制延迟
     */
    public int getRequestsPerSecond() {
        return binance.getRequestsPerSecond();
    }
    
    /**
     * 获取API最大限制
     */
    public int getApiMaxLimit() {
        return binance.getApiMaxLimit();
    }
    
    /**
     * 获取ZIP静态下载基础URL
     */
    public String getDataDownloadBaseUrl() {
        return binance.getDataDownloadBaseUrl();
    }
    
    /**
     * ClickHouse数据源配置
     */
    @Bean
    public DataSource clickHouseDataSource() {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", 
            clickhouse.getHost(), clickhouse.getPort(), clickhouse.getDatabase());
        
        Properties props = new Properties();
        
        // 基础连接配置
        props.setProperty("user", clickhouse.getUsername());
        props.setProperty("password", clickhouse.getPassword());
        
        // 连接池配置
        props.setProperty("max_connections", String.valueOf(connectionPool.getMaxConnections()));
        props.setProperty("connection_timeout", String.valueOf(connectionPool.getConnectionTimeout()));
        props.setProperty("socket_timeout", String.valueOf(connectionPool.getSocketTimeout()));
        
        // 批量写入优化
        props.setProperty("max_insert_block_size", String.valueOf(dataBuffer.getBatchSize()));
        props.setProperty("max_query_size", "1048576");
        props.setProperty("max_result_rows", "1000000");
        
        // 内存管理
        props.setProperty("max_memory_usage", "2000000000");
        props.setProperty("max_bytes_before_external_group_by", "1000000000");
        props.setProperty("max_bytes_before_external_sort", "1000000000");
        
        // 网络优化
        props.setProperty("compress", "1");
        props.setProperty("decompress", "1");
        props.setProperty("max_compress_block_size", "1048576");
        
        // 时间处理
        props.setProperty("use_server_time_zone", "false");
        props.setProperty("use_time_zone", "UTC");
        
        // 查询优化
        props.setProperty("extremes", "0");
        props.setProperty("readonly", "0");
        props.setProperty("max_execution_time", "300");
        
        // 高级性能调优
        props.setProperty("max_threads", "8");
        props.setProperty("max_insert_threads", "4");
        props.setProperty("max_final_threads", "4");
        props.setProperty("log_queries", "0");
        props.setProperty("log_query_threads", "0");
        
        // 错误处理
        props.setProperty("interactive_delay", "4000000");
        props.setProperty("connect_timeout_with_failover_ms", "50");
        props.setProperty("connect_timeout_with_failover_secure_ms", "100");

        // 异步插入（提升吞吐）
        props.setProperty("async_insert", "1");
        props.setProperty("wait_for_async_insert", "0");

        // 使用HikariCP构建连接池
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(url);
        cfg.setDriverClassName("com.clickhouse.jdbc.ClickHouseDriver");
        cfg.setUsername(clickhouse.getUsername());
        cfg.setPassword(clickhouse.getPassword());
        cfg.setPoolName("clickhouse-pool");

        // 建议补充：池级别校验与自动提交设置
        cfg.setConnectionTestQuery("SELECT 1");
        cfg.setAutoCommit(true);

        // 池参数映射
        cfg.setMaximumPoolSize(connectionPool.getMaxConnections());
        cfg.setMinimumIdle(connectionPool.getMinConnections());
        cfg.setConnectionTimeout(connectionPool.getConnectionTimeout());
        cfg.setIdleTimeout(connectionPool.getMaxIdleTime());
        cfg.setValidationTimeout(connectionPool.getValidationTimeout());

        // ClickHouse驱动参数注入到DataSource
        for (String name : props.stringPropertyNames()) {
            cfg.addDataSourceProperty(name, props.getProperty(name));
        }

        HikariDataSource ds = new HikariDataSource(cfg);
        log.info("ClickHouse数据源(HikariCP)配置完成: {}:{}/{}",
            clickhouse.getHost(), clickhouse.getPort(), clickhouse.getDatabase());
        return ds;
    }
}