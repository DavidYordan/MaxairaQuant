package com.maxaira.scheduler;

import com.maxaira.model.TradingPairConfig;
import com.maxaira.service.BackfillTaskManager;
import com.maxaira.service.TradingPairConfigService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class GapHealScheduler {

    private final BackfillTaskManager backfillTaskManager;
    private final TradingPairConfigService tradingPairConfigService;
    private final org.springframework.scheduling.TaskScheduler taskScheduler;

    @EventListener(ApplicationReadyEvent.class)
    public void onAppReady() {
        log.info("应用已就绪，启动缺口修复调度");
        runMinuteGaps();
        runHourGaps();
        taskScheduler.scheduleAtFixedRate(this::runMinuteGaps, Duration.ofSeconds(30));
        taskScheduler.scheduleAtFixedRate(this::runHourGaps, Duration.ofMinutes(5));
    }
    
    // 任务主体（分钟）
    private void runMinuteGaps() {
        try {
            List<TradingPairConfig> enabled = tradingPairConfigService.getAllEnabledConfigs();
            for (TradingPairConfig cfg : enabled) {
                backfillTaskManager.submitInterval(cfg.getSymbol(), cfg.getMarketType(), "1m");
            }
        } catch (Exception e) {
            log.warn("GapHealScheduler 执行失败", e);
        }
    }

    // 任务主体（小时）
    private void runHourGaps() {
        try {
            List<TradingPairConfig> enabled = tradingPairConfigService.getAllEnabledConfigs();
            for (TradingPairConfig cfg : enabled) {
                backfillTaskManager.submitInterval(cfg.getSymbol(), cfg.getMarketType(), "1h");
            }
        } catch (Exception e) {
            log.warn("GapHealScheduler 执行失败", e);
        }
    }
}