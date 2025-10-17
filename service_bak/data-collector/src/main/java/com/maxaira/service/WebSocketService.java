package com.maxaira.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxaira.config.ApplicationConfig;
import com.maxaira.model.Kline;
import com.maxaira.websocket.ClientBroadcastService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 设计原则：
 * - 心跳检测与指数退避重连
 * - 与 DataBufferService 协作批量落库
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketService {

    private final ApplicationConfig config;
    private final DataBufferService bufferService;
    private final ClientBroadcastService broadcastService;

    // 缓存的配置参数 - 避免高频调用config.getXxx()
    private long heartbeatTimeoutMs;
    private long reconnectDelayMs;
    private int maxReconnectAttempts;

    // 优化的OkHttp客户端 - 针对WebSocket长连接场景
    private final OkHttpClient client = new OkHttpClient.Builder()
            .connectTimeout(30, TimeUnit.SECONDS)           // 连接超时
            .readTimeout(0, TimeUnit.SECONDS)               // WebSocket不设置读超时
            .writeTimeout(30, TimeUnit.SECONDS)             // 写超时
            .pingInterval(30, TimeUnit.SECONDS)             // WebSocket心跳间隔
            .retryOnConnectionFailure(true)                 // 连接失败重试
            .connectionPool(new ConnectionPool(
                    20,                                     // 最大空闲连接数
                    5, TimeUnit.MINUTES                     // 连接保活时间
            ))
            .dispatcher(new Dispatcher(
                    Executors.newFixedThreadPool(8, r -> {  // WebSocket专用线程池
                        Thread t = new Thread(r, "okhttp-websocket");
                        t.setDaemon(true);
                        return t;
                    })
            ))
            .build();
    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<StreamKey, ManagedStream> streams = new ConcurrentHashMap<>();
    private ScheduledExecutorService heartbeatScheduler;

    @PostConstruct
    public void init() {
        // 缓存配置参数，避免高频调用config.getXxx()
        this.heartbeatTimeoutMs = config.getHeartbeatTimeout();
        this.reconnectDelayMs = config.getReconnectDelay();
        this.maxReconnectAttempts = config.getMaxReconnectAttempts();
        
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ws-heartbeat"));
        heartbeatScheduler.scheduleAtFixedRate(this::checkHeartbeats, 
                5, 5, TimeUnit.SECONDS);
        log.info("WebSocketService initialized with cached config: heartbeatTimeout={}ms, reconnectDelay={}ms, maxReconnectAttempts={}", 
                heartbeatTimeoutMs, reconnectDelayMs, maxReconnectAttempts);
    }

    @PreDestroy
    public void shutdown() {
        if (heartbeatScheduler != null) {
            heartbeatScheduler.shutdown();
        }
        streams.values().forEach(ms -> {
            if (ms.socket != null) ms.socket.close(1000, "shutdown");
        });
    }

    /**
     * 启动指定 symbol/market/period 的订阅
     */
    public void start(String symbol, String marketType, String period) {
        StreamKey key = new StreamKey(symbol, marketType, period);
        ManagedStream existing = streams.get(key);
        if (existing != null && existing.socket != null) {
            log.info("Stream already running: {} {} {}", symbol, marketType, period);
            return;
        }
    
        String url = config.buildKlineUrl(symbol, marketType, period);
        Request request = new Request.Builder().url(url).build();
    
        ManagedStream ms = (existing != null) ? existing : new ManagedStream();
        ms.key = key;
        // 保留重试计数；onOpen 时归零
        WebSocketListener listener = new WebSocketListener() {
            @Override
            public void onOpen(WebSocket webSocket, Response response) {
                ms.socket = webSocket;
                ms.lastMessageMs = System.currentTimeMillis();
                ms.attempt = 0;
                log.info("WS opened: {} {} {} -> {}", symbol, marketType, period, url);
            }
            @Override
            public void onMessage(WebSocket webSocket, String text) {
                ms.lastMessageMs = System.currentTimeMillis();
                handleMessage(symbol, marketType, period, text);
            }
            @Override
            public void onClosed(WebSocket webSocket, int code, String reason) {
                log.warn("WS closed: {} {} {} code={}, reason={}", symbol, marketType, period, code, reason);
                scheduleReconnect(ms, symbol, marketType, period);
            }
            @Override
            public void onFailure(WebSocket webSocket, Throwable t, Response response) {
                log.error("WS failure: {} {} {} {}", symbol, marketType, period, t.getMessage(), t);
                scheduleReconnect(ms, symbol, marketType, period);
            }
        };
    
        ms.socket = client.newWebSocket(request, listener);
        streams.put(key, ms);
    }

    /**
     * 停止指定订阅
     */
    public void stop(String symbol, String marketType, String period) {
        StreamKey key = new StreamKey(symbol, marketType, period);
        ManagedStream ms = streams.remove(key);
        if (ms != null && ms.socket != null) {
            ms.socket.close(1000, "manual-stop");
            log.info("WS stopped: {} {} {}", symbol, marketType, period);
        }
    }

    public Map<String, Object> status() {
        return Map.of(
            "connections", streams.size(),
            "streams", streams.entrySet().stream().map(e -> Map.of(
                "symbol", e.getKey().symbol,
                "marketType", e.getKey().marketType,
                "period", e.getKey().period,
                "lastMessageMsAgo", System.currentTimeMillis() - e.getValue().lastMessageMs,
                "attempt", e.getValue().attempt
            )).toList()
        );
    }

    // ============= 内部实现 =============

    private void handleMessage(String symbol, String marketType, String period, String text) {
        try {
            // 先广播原始消息给客户端（包含未收盘的tick）
            broadcastService.broadcastRaw(symbol, marketType, period, text);

            JsonNode root = mapper.readTree(text);
            JsonNode k = root.has("k") ? root.get("k") : root.path("data").path("k");
            if (k.isMissingNode() || k.isNull()) {
                return;
            }
            // 仅收盘时写入
            boolean isClosed = k.path("x").asBoolean(false);
            if (!isClosed) return;

            log.info("Received kline: {}", k);
            Kline kline = Kline.builder()
                    .openTime(k.path("t").asLong(0))
                    .closeTime(k.path("T").asLong(0))
                    .open(new BigDecimal(k.path("o").asText("0")))
                    .high(new BigDecimal(k.path("h").asText("0")))
                    .low(new BigDecimal(k.path("l").asText("0")))
                    .close(new BigDecimal(k.path("c").asText("0")))
                    .volume(new BigDecimal(k.path("v").asText("0")))
                    .quoteVolume(new BigDecimal(k.path("q").asText("0")))
                    .count(k.path("n").asLong(0))
                    .takerBuyBaseVolume(new BigDecimal(k.path("V").asText("0")))
                    .takerBuyQuoteVolume(new BigDecimal(k.path("Q").asText("0")))
                    .build();

            bufferService.bufferKline(symbol, marketType, period, kline);
        } catch (Exception e) {
            log.error("Failed to parse WS message for {} {}: {}", symbol, marketType, e.getMessage(), e);
        }
    }

    private void checkHeartbeats() {
        long now = System.currentTimeMillis();
        for (ManagedStream ms : streams.values()) {
            long dt = now - ms.lastMessageMs;
            if (dt > heartbeatTimeoutMs) {
                log.warn("WS heartbeat timeout for {} {} ({} ms). Reconnecting...", 
                        ms.key.symbol, ms.key.marketType, dt);
                scheduleReconnect(ms, ms.key.symbol, ms.key.marketType, ms.key.period);
            }
        }
    }

    private void scheduleReconnect(ManagedStream ms, String symbol, String marketType, String period) {
        if (ms.socket != null) {
            try { ms.socket.close(1001, "reconnect"); } catch (Exception ignored) {}
            ms.socket = null;
        }
        long delay = Math.min(
                reconnectDelayMs * (1L << Math.min(ms.attempt, 5)), 
                TimeUnit.SECONDS.toMillis(30));
        ms.attempt++;
        if (ms.attempt > maxReconnectAttempts) {
            log.error("Max reconnect attempts reached for {} {}. Giving up.", symbol, marketType);
            streams.remove(ms.key);
            return;
        }
        heartbeatScheduler.schedule(() -> {
            log.info("Reconnecting {} {} {} (attempt {})", symbol, marketType, period, ms.attempt);
            start(symbol, marketType, period);
        }, delay, TimeUnit.MILLISECONDS);
    }

    private static class StreamKey {
        final String symbol;
        final String marketType;
        final String period;

        StreamKey(String symbol, String marketType, String period) {
            this.symbol = symbol.toUpperCase();
            this.marketType = marketType.toLowerCase();
            this.period = period.toLowerCase();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof StreamKey sk)) return false;
            return symbol.equals(sk.symbol) && marketType.equals(sk.marketType) && period.equals(sk.period);
        }

        @Override
        public int hashCode() {
            int h = symbol.hashCode();
            h = h * 31 + marketType.hashCode();
            h = h * 31 + period.hashCode();
            return h;
        }
    }

    private static class ManagedStream {
        StreamKey key;
        WebSocket socket;
        int attempt = 0;
        volatile long lastMessageMs = System.currentTimeMillis();
    }
}