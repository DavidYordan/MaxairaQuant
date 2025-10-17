package com.maxaira.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import com.maxaira.config.ApplicationConfig;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class HighWaterMarkRegistry {

    private final ApplicationConfig applicationConfig;

    private long defaultStartMs() {
        return LocalDate.parse(applicationConfig.getDatabase().getBackfillStartDate())
                .atStartOfDay(ZoneOffset.UTC).toEpochSecond() * 1000L;
    }

    private final ConcurrentMap<String, Long> hwm = new ConcurrentHashMap<>();

    private String key(String symbol, String marketType, String period) {
        return symbol.toLowerCase() + "|" + marketType.toLowerCase() + "|" + period.toLowerCase();
    }

    public Long get(String symbol, String marketType, String period) {
        String k = key(symbol, marketType, period);
        return hwm.computeIfAbsent(k, ignored -> defaultStartMs());
    }

    public void set(String symbol, String marketType, String period, long untilMs) {
        String k = key(symbol, marketType, period);
        hwm.compute(k, (kk, prev) -> prev == null ? untilMs : Math.max(prev, untilMs));
    }

    public void clear(String symbol, String marketType, String period) {
        hwm.remove(key(symbol, marketType, period));
    }
}