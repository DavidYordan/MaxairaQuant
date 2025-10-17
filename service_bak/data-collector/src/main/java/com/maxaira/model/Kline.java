package com.maxaira.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.math.BigDecimal;

/**
 * K线数据模型
 * 
 * 职责：
 * - 表示完整的K线数据结构
 * - 支持所有时间周期（1m/1h）
 * - 包含所有Binance K线字段，确保数据完整性
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Kline {
    
    /**
     * 开盘时间（毫秒，与Binance一致）
     */
    private Long openTime;
    
    /**
     * 收盘时间（毫秒，与Binance一致）
     */
    private Long closeTime;
    
    /**
     * 开盘价
     */
    private BigDecimal open;
    
    /**
     * 最高价
     */
    private BigDecimal high;
    
    /**
     * 最低价
     */
    private BigDecimal low;
    
    /**
     * 收盘价
     */
    private BigDecimal close;
    
    /**
     * 成交量
     */
    private BigDecimal volume;
    
    /**
     * 成交额
     */
    private BigDecimal quoteVolume;
    
    /**
     * 成交笔数
     */
    private Long count;
    
    /**
     * 主动买入成交量
     */
    private BigDecimal takerBuyBaseVolume;
    
    /**
     * 主动买入成交额
     */
    private BigDecimal takerBuyQuoteVolume;
    
    /**
     * 验证K线数据的完整性（基于毫秒时间戳）
     */
    public boolean isValid() {
        return openTime != null
            && closeTime != null
            && open != null 
            && high != null 
            && low != null 
            && close != null
            && volume != null
            && quoteVolume != null
            && count != null
            && takerBuyBaseVolume != null
            && takerBuyQuoteVolume != null
            && openTime < closeTime
            && high.compareTo(low) >= 0
            && high.compareTo(open) >= 0
            && high.compareTo(close) >= 0
            && low.compareTo(open) <= 0
            && low.compareTo(close) <= 0;
    }
}