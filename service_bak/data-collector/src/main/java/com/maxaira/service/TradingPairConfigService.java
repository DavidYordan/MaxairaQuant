package com.maxaira.service;

import com.maxaira.model.TradingPairConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 交易对配置管理服务
 * 
 * 职责：
 * - 交易对配置的CRUD操作
 * - 配置启停管理
 * - 配置变更通知
 * - 配置验证
 * 
 * 设计原则：
 * - 单一职责：专注于配置管理
 * - 事件驱动：配置变更时通知相关服务
 * - 数据一致性：确保配置变更的原子性
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TradingPairConfigService {
    
    private final DataSource dataSource;
    private final WebSocketService webSocketService;
    private static final List<String> DEFAULT_PERIODS = List.of("1m", "1h");
    private final BackfillTaskManager backfillTaskManager;
    
    /**
     * 获取所有启用的配置
     */
    public List<TradingPairConfig> getAllEnabledConfigs() throws SQLException {
        String sql = "SELECT * FROM trading_pair_config WHERE enabled = 1 ORDER BY symbol, market_type";
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            List<TradingPairConfig> configs = new ArrayList<>();
            while (rs.next()) {
                configs.add(mapResultSetToConfig(rs));
            }
            return configs;
        }
    }
    
    /**
     * 获取所有配置（包括禁用的）
     */
    public List<TradingPairConfig> getAllConfigs() throws SQLException {
        String sql = "SELECT * FROM trading_pair_config ORDER BY symbol, market_type";
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            List<TradingPairConfig> configs = new ArrayList<>();
            while (rs.next()) {
                configs.add(mapResultSetToConfig(rs));
            }
            return configs;
        }
    }
    
    /**
     * 添加配置（默认禁用）
     */
    public void addConfig(String symbol, String marketType) throws SQLException {
        if (configExists(symbol, marketType)) {
            throw new SQLException("配置已存在: " + symbol + " " + marketType);
        }
        String sql = "INSERT INTO trading_pair_config (id, symbol, market_type, enabled) VALUES (?, ?, ?, 0)";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setLong(1, getNextId(connection));
            stmt.setString(2, symbol.toUpperCase());
            stmt.setString(3, marketType.toLowerCase());
            int inserted = stmt.executeUpdate();
            if (inserted > 0) {
                log.info("配置添加成功（默认禁用）：{} {}", symbol, marketType);
            }
        }
    }
    
    /**
     * 处理配置启用
     */
    private void handleConfigEnabled(TradingPairConfig config) throws SQLException {
        for (String period : DEFAULT_PERIODS) {
            try {
                webSocketService.start(config.getSymbol(), config.getMarketType(), period);
                log.info("配置已启用并启动WebSocket连接：{} {} {}", 
                    config.getSymbol(), config.getMarketType(), period);
            } catch (Exception e) {
                log.error("启动WebSocket连接失败：{} {} {}", 
                    config.getSymbol(), config.getMarketType(), period, e);
            }
            backfillTaskManager.submitInterval(config.getSymbol(), config.getMarketType(), period);
        }
        log.info("已启动历史补采任务（1m/1h）：{} {}", config.getSymbol(), config.getMarketType());
    }

    private void handleConfigDisabled(TradingPairConfig config) {
        for (String period : DEFAULT_PERIODS) {
            try {
                webSocketService.stop(config.getSymbol(), config.getMarketType(), period);
                log.info("配置已禁用并停止WS（保留历史数据表）：{} {} {}", config.getSymbol(), config.getMarketType(), period);
            } catch (Exception e) {
                log.error("停止WS失败：{} {} {}", config.getSymbol(), config.getMarketType(), period, e);
            }
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        try {
            autoStartEnabledStreams();
        } catch (Exception e) {
            log.error("应用启动后自动启动启用配置失败", e);
        }
    }

    private void autoStartEnabledStreams() throws SQLException {
        List<TradingPairConfig> enabledConfigs = getAllEnabledConfigs();
        for (TradingPairConfig cfg : enabledConfigs) {
            for (String period : DEFAULT_PERIODS) {
                try {
                    webSocketService.start(cfg.getSymbol(), cfg.getMarketType(), period);
                    log.info("应用启动后自动启动 WS：{} {} {}", 
                        cfg.getSymbol(), cfg.getMarketType(), period);
                } catch (Exception e) {
                    log.error("自动启动 WS 失败：{} {} {}", cfg.getSymbol(), cfg.getMarketType(), period, e);
                }
                backfillTaskManager.submitInterval(cfg.getSymbol(), cfg.getMarketType(), period);
            }
            log.info("应用启动后自动启动历史补采（1m/1h）：{} {}", cfg.getSymbol(), cfg.getMarketType());
        }
        log.info("自动启动完成：共处理启用配置 {} 个", enabledConfigs.size());
    }

    /**
     * 根据ID获取配置
     */
    public TradingPairConfig getConfigById(long id) throws SQLException {
        String sql = "SELECT * FROM trading_pair_config WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            
            stmt.setLong(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToConfig(rs);
                }
                return null;
            }
        }
    }
    
    /**
     * 检查配置是否存在
     */
    private boolean configExists(String symbol, String marketType) throws SQLException {
        String sql = "SELECT COUNT(*) FROM trading_pair_config WHERE symbol = ? AND market_type = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, symbol.toUpperCase());
            stmt.setString(2, marketType.toLowerCase());
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }
    
    /**
     * 获取下一个ID
     */
    private long getNextId(Connection connection) throws SQLException {
        String sql = "SELECT MAX(id) FROM trading_pair_config";
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1) + 1;
            }
            return 1;
        }
    }
    
    /**
     * 映射ResultSet到配置对象
     */
    private TradingPairConfig mapResultSetToConfig(ResultSet rs) throws SQLException {
        TradingPairConfig config = new TradingPairConfig();
        config.setId(rs.getLong("id"));
        config.setSymbol(rs.getString("symbol"));
        config.setMarketType(rs.getString("market_type"));
        config.setEnabled(rs.getInt("enabled") == 1);
        return config;
    }
    
    public void toggleConfig(long id, boolean enabled) throws SQLException {
        TradingPairConfig config = getConfigById(id);
        if (config == null) {
            throw new SQLException("配置不存在，ID: " + id);
        }
        if (config.getEnabled() == enabled) {
            log.info("配置状态无需变更：{} {} 已经是 {}", 
                config.getSymbol(), config.getMarketType(), 
                enabled ? "启用" : "禁用");
            return;
        }

        String sql = "ALTER TABLE trading_pair_config UPDATE enabled = ? WHERE id = ?";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setInt(1, enabled ? 1 : 0);
            stmt.setLong(2, id);
            int updated = stmt.executeUpdate();

            if (updated > 0) {
                if (enabled) {
                    handleConfigEnabled(config);
                } else {
                    handleConfigDisabled(config);
                }
            }
        }
    }
}