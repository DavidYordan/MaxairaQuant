package com.maxaira.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

/**
 * 交易对配置模型 - 简化版
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradingPairConfig {
    
    private Long id;
    private String symbol;        // BTCUSDT, ETHUSDT
    private String marketType;    // spot, futures
    private Boolean enabled;
    
    /**
     * 获取表名
     */
    public String getTableName() {
        return String.format("kline_%s_%s", symbol.toLowerCase(), marketType.toLowerCase());
    }
    
    /**
     * 验证配置有效性
     */
    public boolean isValid() {
        return symbol != null && !symbol.trim().isEmpty() 
            && marketType != null && !marketType.trim().isEmpty()
            && enabled != null;
    }
}