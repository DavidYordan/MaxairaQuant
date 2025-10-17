package com.maxaira.controller;

import com.maxaira.service.TradingPairConfigService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/config")
@RequiredArgsConstructor
public class TradingPairConfigController {
    
    private final TradingPairConfigService configService;
    
    /**
     * 获取所有启用的配置
     */
    @GetMapping
    public ResponseEntity<?> getAllConfigs() {
        try {
            return ResponseEntity.ok(Map.of(
                "success", true,
                "data", configService.getAllEnabledConfigs()
            ));
        } catch (Exception e) {
            log.error("获取配置失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", e.getMessage()
            ));
        }
    }

    /**
     * 获取所有配置（包括禁用的）
     */
    @GetMapping("/all")
    public ResponseEntity<?> getAllConfigsIncludingDisabled() {
        try {
            return ResponseEntity.ok(Map.of(
                "success", true,
                "data", configService.getAllConfigs()
            ));
        } catch (Exception e) {
            log.error("获取所有配置失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", e.getMessage()
            ));
        }
    }

    /**
     * 添加配置（默认禁用）
     */
    @PostMapping
    public ResponseEntity<?> addConfig(@RequestParam String symbol,
                                       @RequestParam String marketType) {
        try {
            configService.addConfig(symbol, marketType);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "配置添加成功（默认禁用状态）"
            ));
        } catch (Exception e) {
            log.error("添加配置失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", e.getMessage()
            ));
        }
    }

    /**
     * 智能启用/禁用配置
     *
     * 核心功能：
     * - 启用时：创建 1m/1h K线表 + 启动所有 WS 连接
     * - 禁用时：停止所有 WS 连接 + 保留历史数据
     */
    @PostMapping("/{id}/toggle")
    public ResponseEntity<?> toggleConfig(@PathVariable Long id,
                                          @RequestParam Boolean enabled) {
        try {
            configService.toggleConfig(id, enabled);
            return ResponseEntity.ok(Map.of(
                "success", true,
                "message", enabled ? "配置已启用并自动启动四周期 WS" : "配置已禁用并自动停止四周期 WS"
            ));
        } catch (Exception e) {
            log.error("切换配置状态失败", e);
            return ResponseEntity.internalServerError().body(Map.of(
                "success", false,
                "message", e.getMessage()
            ));
        }
    }
}