package com.maxaira.service;

import com.maxaira.config.ApplicationConfig;
import com.maxaira.model.Kline;
import com.maxaira.model.ProxyConfig;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

/**
 * 数据库服务 - 统一管理所有数据库操作
 * 
 * 职责：
 * - 应用启动时初始化数据库
 * - 管理交易对配置
 * - 管理K线数据表
 * - 提供数据查询和插入功能
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DatabaseService implements ApplicationRunner {
    
    private final DataSource dataSource;
    private final ApplicationConfig applicationConfig;

    private List<String> mainStreamSymbols;
    private List<String> marketTypes;
    private int batchSize;

    // 初始化
    @PostConstruct
    private void init() {
        mainStreamSymbols = applicationConfig.getDatabase().getMainStreamSymbols();
        marketTypes = applicationConfig.getDatabase().getMarketTypes();
        batchSize = applicationConfig.getDataBuffer().getBatchSize();
    }
    
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("开始数据库初始化...");
        try (Connection connection = dataSource.getConnection()) {

            // 1) 代理配置表
            createProxyConfigTable(connection);

            // 2) 预建所有K线表
            createAllKlineTablesFromConfigLists(connection);

            // 3) 交易对配置表
            createConfigTable(connection);

            // 4) 初始化默认配置
            initializeDefaultConfigs(connection);

            log.info("数据库初始化完成");
        } catch (Exception e) {
            log.error("数据库初始化失败", e);
            throw e;
        }
    }
    
    // ==================== 配置管理 ====================
    
    /**
     * 创建交易对配置表
     */
    private void createConfigTable(Connection connection) throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS trading_pair_config (
                id UInt64,
                symbol String,
                market_type String,
                enabled UInt8 DEFAULT 1,
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (symbol, market_type)
            """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("交易对配置表创建完成");
        }
    }
    
    /**
     * 初始化默认配置
     * 注意：初始化大量主流币种，仅ETH合约系列默认启用，其它全部禁用
     */
    private void initializeDefaultConfigs(Connection connection) throws SQLException {
        if (hasExistingConfigs(connection)) {
            log.info("配置表已有数据，跳过初始化");
            return;
        }
    
        String sql = """
            INSERT INTO trading_pair_config (id, symbol, market_type, enabled)
            VALUES (?, ?, ?, ?)
            """;
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            long id = 1;
            int enabledCount = 0;
            int totalCount = 0;
    
            for (String symbol : mainStreamSymbols) {
                for (String marketTypeRaw : marketTypes) {
                    String marketType = marketTypeRaw.toLowerCase();
                    boolean enabled = "ETHUSDT".equals(symbol) && "um".equals(marketType);
                    stmt.setLong(1, id++);
                    stmt.setString(2, symbol.toUpperCase());
                    stmt.setString(3, marketType);
                    stmt.setInt(4, enabled ? 1 : 0);
                    stmt.addBatch();
    
                    totalCount++;
                    if (enabled) enabledCount++;
                }
            }
            stmt.executeBatch();
            log.info("默认配置初始化完成：{} 个币种，{} 个市场类型，共 {} 条记录，其中 {} 条启用（ETHUSDT UM）",
                mainStreamSymbols.size(), marketTypes.size(), totalCount, enabledCount);
        }
    }
    
    private boolean hasExistingConfigs(Connection connection) throws SQLException {
        String sql = "SELECT COUNT(*) FROM trading_pair_config LIMIT 1";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() && rs.getInt(1) > 0;
        }
    }
    
    // ==================== 表管理 ====================

    /**
     * 创建代理配置表
     */
    private void createProxyConfigTable(Connection connection) throws SQLException {
        String sql = """
            CREATE TABLE IF NOT EXISTS proxy_config (
                id UInt64,
                host String,
                port UInt16,
                username String,
                password String,
                enabled UInt8 DEFAULT 1,
                created_at DateTime DEFAULT now(),
                updated_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY (host, port)
            """;
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
            log.info("代理配置表创建完成");
        }
    }

    /**
     * 按 ApplicationConfig 列表预建所有 K 线表（幂等）
     */
    private void createAllKlineTablesFromConfigLists(Connection connection) throws SQLException {
        // 固定两个周期
        List<String> periods = List.of("1m", "1h");

        int total = 0;
        for (String symbol : mainStreamSymbols) {
            for (String marketTypeRaw : marketTypes) {
                String marketType = marketTypeRaw.toLowerCase();
                for (String period : periods) {
                    String tableName = getTableName(symbol, marketType, period);
                    createKlineTable(connection, tableName);
                    total++;
                }
            }
        }
        log.info("预建K线表完成（固定周期 1m/1h）：共 {} 张表", total);
    }
    
    /**
     * 创建K线表
     */
    private void createKlineTable(Connection connection, String tableName) throws SQLException {
        String sql = String.format("""
            CREATE TABLE IF NOT EXISTS %s (
                open_time UInt64,
                close_time UInt64,
                open Decimal128(8),
                high Decimal128(8),
                low Decimal128(8),
                close Decimal128(8),
                volume Decimal128(8),
                quote_volume Decimal128(8),
                count UInt64,
                taker_buy_base_volume Decimal128(8),
                taker_buy_quote_volume Decimal128(8),
                version UInt64 DEFAULT toUInt64(toUnixTimestamp(now()))
            ) ENGINE = ReplacingMergeTree(version)
            PARTITION BY toYYYYMM(toDateTime(open_time / 1000))
            ORDER BY open_time
            """, tableName);
    
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ==================== 数据操作 ====================
    
    /**
     * 批量插入K线数据 - 高性能版本（写入前两层去重：批内 + 跨表）
     */
    public void batchInsertKlines(String tableName, List<Kline> klines) throws SQLException {
        if (klines == null || klines.isEmpty()) return;
        Map<Long, Kline> byOpenTime = new LinkedHashMap<>();
        for (Kline k : klines) {
            byOpenTime.put(k.getOpenTime(), k);
        }
        List<Kline> uniqueBatch = new ArrayList<>(byOpenTime.values());
        if (uniqueBatch.isEmpty()) {
            log.info("批插入唯一化后为空: {} 原始条数={} 唯一条数=0", tableName, klines.size());
            return;
        }
        long minOpen = uniqueBatch.stream().mapToLong(Kline::getOpenTime).min().orElse(0L);
        long maxOpen = uniqueBatch.stream().mapToLong(Kline::getOpenTime).max().orElse(0L);
        String sql = String.format("""
            INSERT INTO %s (
                open_time, close_time, open, high, low, close,
                volume, quote_volume, count, taker_buy_base_volume, taker_buy_quote_volume
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, tableName);

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {

            int batchCount = 0;

            for (Kline kline : uniqueBatch) {
                stmt.setLong(1, kline.getOpenTime());
                stmt.setLong(2, kline.getCloseTime());
                stmt.setBigDecimal(3, kline.getOpen());
                stmt.setBigDecimal(4, kline.getHigh());
                stmt.setBigDecimal(5, kline.getLow());
                stmt.setBigDecimal(6, kline.getClose());
                stmt.setBigDecimal(7, kline.getVolume());
                stmt.setBigDecimal(8, kline.getQuoteVolume());
                stmt.setLong(9, kline.getCount());
                stmt.setBigDecimal(10, kline.getTakerBuyBaseVolume());
                stmt.setBigDecimal(11, kline.getTakerBuyQuoteVolume());
                stmt.addBatch();

                batchCount++;
                if (batchCount >= batchSize) {
                    stmt.executeBatch();
                    stmt.clearBatch();
                    batchCount = 0;
                }
            }

            if (batchCount > 0) stmt.executeBatch();
            log.info("批插入完成: {} 插入条数={} 范围=[{} - {}]", tableName, uniqueBatch.size(), minOpen, maxOpen);
        }
    }

    public List<long[]> findGapsWindowed(String tableName, long startOpenMs, long endOpenMs, long stepMs) throws SQLException {
        List<long[]> gaps = new ArrayList<>();

        // 1) 相邻数据点之间的缺口
        String windowSql = String.format("""
            SELECT (prev_open_time + %d) AS gap_start, (open_time - %d) AS gap_end
            FROM (
              SELECT open_time,
                     lag(open_time) OVER (ORDER BY open_time) AS prev_open_time
              FROM %s
              WHERE open_time >= ? AND open_time < ?
              ORDER BY open_time
            )
            WHERE prev_open_time IS NOT NULL AND (open_time - prev_open_time) > %d
            ORDER BY open_time
            """, stepMs, stepMs, tableName, stepMs);
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(windowSql)) {
            stmt.setLong(1, startOpenMs);
            stmt.setLong(2, endOpenMs);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long gapStart = rs.getLong(1);
                    long gapEnd = rs.getLong(2);
                    if (gapStart < startOpenMs) continue;
                    gaps.add(new long[]{gapStart, gapEnd});
                    log.info("发现窗口内相邻数据点缺口: {} - {}", gapStart, gapEnd);
                }
            }
        }

        // 2) 边界首尾：使用 ORDER BY + LIMIT 1，避免空区间 min/max 误判为 0
        String minSql = String.format("""
            SELECT open_time
            FROM %s
            WHERE open_time >= ? AND open_time <= ?
            ORDER BY open_time ASC
            LIMIT 1
            """, tableName);
        String maxSql = String.format("""
            SELECT open_time
            FROM %s
            WHERE open_time >= ? AND open_time <= ?
            ORDER BY open_time DESC
            LIMIT 1
            """, tableName);

        Long minOpen = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(minSql)) {
            stmt.setLong(1, startOpenMs);
            stmt.setLong(2, endOpenMs);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    minOpen = rs.getLong(1);
                }
            }
        }

        Long maxOpen = null;
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(maxSql)) {
            stmt.setLong(1, startOpenMs);
            stmt.setLong(2, endOpenMs);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    maxOpen = rs.getLong(1);
                }
            }
        }

        // 区间内没有任何数据点，整段视为缺口
        if (minOpen == null || maxOpen == null) {
            log.info("区间内没有任何数据点，整段视为缺口: {} - {}", startOpenMs, endOpenMs);
            gaps.clear();
            gaps.add(new long[]{startOpenMs, endOpenMs});
            return gaps;
        }

        // 3) 领先缺口（start 到首条数据前），端点排除
        if (minOpen > startOpenMs) {
            long leadEnd = minOpen - stepMs;
            if (leadEnd >= startOpenMs) {
                gaps.add(new long[]{startOpenMs, leadEnd});
            }
        }

        // 4) 尾随缺口（最后一条之后到 end），以最后一条 + step 为起点
        long nextExpected = maxOpen + stepMs;
        if (nextExpected <= endOpenMs) {
            log.info("发现尾随缺口: {} - {}", nextExpected, endOpenMs);
            gaps.add(new long[]{nextExpected, endOpenMs});
        }

        // 缺口按起点排序，避免重复或乱序
        gaps.sort(Comparator.comparingLong(a -> a[0]));
        return gaps;
    }

    public Optional<ProxyConfig> getProxyConfig() {
        String sql = "SELECT host, port, username, password FROM proxy_config WHERE enabled = 1 ORDER BY id LIMIT 1";
        try (Connection conn = dataSource.getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {
            if (rs.next()) {
                ProxyConfig cfg = ProxyConfig.builder()
                        .host(rs.getString("host"))
                        .port(rs.getInt("port"))
                        .username(rs.getString("username"))
                        .password(rs.getString("password"))
                        .build();
                return Optional.of(cfg);
            }
        } catch (SQLException e) {
            log.error("加载 SOCKS5 代理配置失败", e);
        }
        return Optional.empty();
    }
    
    // ==================== 工具方法 ====================
    
    public String getTableName(String symbol, String marketType, String period) {
        return String.format("kline_%s_%s_%s", 
            symbol.toLowerCase(), 
            marketType.toLowerCase(),
            period.toLowerCase());
    }

    // 获取全表最大 open_time；空表返回 null
    public Long getMaxOpenTime(String tableName) throws SQLException {
        String sql = String.format("SELECT max(open_time) FROM %s", tableName);
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                long v = rs.getLong(1);
                if (rs.wasNull()) return null;
                return v;
            }
        }
        return null;
    }

    // 统计某闭区间内的行数（包含端点）
    public long countRowsBetween(String tableName, long startOpenMs, long endOpenMs) throws SQLException {
        String sql = String.format(
            "SELECT COUNT(DISTINCT open_time) FROM %s WHERE open_time >= ? AND open_time <= ?",
            tableName
        );
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, startOpenMs);
            ps.setLong(2, endOpenMs);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getLong(1);
            }
        }
        return 0L;
    }
}