package com.maxaira.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.maxaira.config.ApplicationConfig;
import com.maxaira.model.Kline;
import com.maxaira.model.ProxyConfig;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@Service
public class BackfillService {

    private final ApplicationConfig config;
    private final DatabaseService databaseService;
    private final DataBufferService dataBufferService;
    private final HighWaterMarkRegistry highWaterMarkRegistry;
    private final Set<String> routeReady = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final ObjectMapper objectMapper = new ObjectMapper();

    // HTTP clients
    private OkHttpClient defaultClient;
    private OkHttpClient proxyClient;

    // market types permanently using proxy after first 403/451
    private final Set<String> marketTypesUsingProxy = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // rate limit & retries
    private int apiMaxLimit;       // Binance REST klines typical max limit
    private int restRetries = 3;          // per request
    private final ApiRateLimiter apiRateLimiter;

    @Qualifier("backfillExecutor")
    private final ThreadPoolTaskExecutor backfillExecutor;
    @Qualifier("windowExecutor")
    private final ThreadPoolTaskExecutor windowExecutor;

    // 同表补采自锁：防止同一张表被并发补采
    private final Set<String> intervalLocks = Collections.newSetFromMap(new ConcurrentHashMap<>());

    @PostConstruct
    public void init() {
        // build default client
        defaultClient = new OkHttpClient.Builder()
                .connectTimeout(20, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .callTimeout(120, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .addInterceptor(chain -> {
                    Request originalRequest = chain.request();
                    Request newRequest = originalRequest.newBuilder()
                            .header("Accept-Encoding", "identity")
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                            .build();
                    return chain.proceed(newRequest);
                })
                .build();

        // 从配置读取速率与限制，统一由 yml 控制
        try {
            this.apiMaxLimit = config.getApiMaxLimit();
        } catch (Exception ignored) {}

        // 加载代理配置（含 username/password），仅用于本次运行，不做使用策略的持久化
        try {
            Optional<ProxyConfig> pc = databaseService.getProxyConfig();
            pc.ifPresent(cfg -> {
                // 这里改为构建 HTTP 代理客户端（替换原来的 buildSocksClient）
                proxyClient = buildHttpProxyClient(cfg);
                log.info("加载代理配置: {}:{} (HTTP 代理 + 认证)", cfg.getHost(), cfg.getPort());
            });
        } catch (Exception e) {
            log.warn("加载代理配置失败", e);
        }
    }

    // ========== Core Backfill (REST-only) ==========

    // 新增：窗口结果包装，便于按完成顺序处理
    private static class WindowResult {
        final List<Kline> klines;
        WindowResult(long[] window, List<Kline> klines) {
            this.klines = klines;
        }
    }

    // 显式构造方法：在参数上使用 Qualifier
    public BackfillService(
            ApplicationConfig config,
            DatabaseService databaseService,
            DataBufferService dataBufferService,
            HighWaterMarkRegistry highWaterMarkRegistry,
            ApiRateLimiter apiRateLimiter,
            @Qualifier("backfillExecutor")
            ThreadPoolTaskExecutor backfillExecutor,
            @Qualifier("windowExecutor")
            ThreadPoolTaskExecutor windowExecutor
    ) {
        this.config = config;
        this.databaseService = databaseService;
        this.dataBufferService = dataBufferService;
        this.highWaterMarkRegistry = highWaterMarkRegistry;
        this.apiRateLimiter = apiRateLimiter;
        this.backfillExecutor = backfillExecutor;
        this.windowExecutor = windowExecutor;
    }

    public CompletableFuture<Void> backfillInterval(String symbol, String marketType, String period) {
        String tableName = config.getTableName(symbol, marketType, period);
        // 自锁：如果同表已有任务运行，直接跳过
        if (!intervalLocks.add(tableName)) {
            return CompletableFuture.completedFuture(null);
        }
        try {
            long stepMs = getPeriodStepSeconds(period) * 1000L;

            long startOpenMs = highWaterMarkRegistry.get(symbol, marketType, period);
            startOpenMs = alignToStep(startOpenMs, stepMs);

            long endOpenMs = System.currentTimeMillis() - stepMs;
            endOpenMs = alignToStep(endOpenMs, stepMs);

            if (endOpenMs < startOpenMs) {
                intervalLocks.remove(tableName);
                return CompletableFuture.completedFuture(null);
            }

            List<long[]> gaps = databaseService.findGapsWindowed(tableName, startOpenMs, endOpenMs, stepMs);

            if (gaps.isEmpty()) {
                highWaterMarkRegistry.set(symbol, marketType, period, endOpenMs + stepMs);
                intervalLocks.remove(tableName);
                return CompletableFuture.completedFuture(null);
            }

            log.info("检测到 {} 个缺口: {}", gaps.size(), gaps.stream()
                    .map(gap -> String.format("[%d-%d]", gap[0], gap[1]))
                    .collect(Collectors.toList()));

            AtomicInteger remaining = new AtomicInteger(gaps.size());
            CompletableFuture<Void> allDone = new CompletableFuture<>();

            for (long[] gap : gaps) {
                CompletableFuture
                        .runAsync(() -> {
                            try {
                                fillGapViaRestParallel(symbol, marketType, period, tableName, gap[0], gap[1], stepMs);
                            } catch (Exception e) {
                                log.error("并发补采失败 {} {} {} gap[{}-{}]: {}", symbol, marketType, period, gap[0], gap[1], e.getMessage());
                            }
                        }, backfillExecutor)
                        .whenComplete((v, ex) -> {
                            if (ex != null) {
                                log.error("缺口任务异常完成 {} {} {} gap[{}-{}]: {}", symbol, marketType, period, gap[0], gap[1], ex.getMessage());
                            }
                            if (remaining.decrementAndGet() == 0) {
                                intervalLocks.remove(tableName);
                                log.info("所有缺口并发任务已完成: {} {} {} 缺口数={}", symbol, marketType, period, gaps.size());
                                allDone.complete(null);
                            }
                        });
            }
            return allDone;
        } catch (Exception e) {
            log.error("backfillInterval 执行失败: {} {} {}: {}", symbol, marketType, period, e);
            intervalLocks.remove(tableName);
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(e);
            return failed;
        }
    }

    private void fillGapViaRestParallel(String symbol, String marketType, String period, String tableName,
                                        long gapStartMs, long gapEndMs, long stepMs) throws Exception {
        long windowMs = stepMs * apiMaxLimit;
        List<long[]> windows = new ArrayList<>();
        long cursor = gapStartMs;
    
        while (cursor <= gapEndMs) {
            long lastOpenMsInWindow = Math.min(cursor + windowMs - stepMs, gapEndMs);
            long endMsParam = lastOpenMsInWindow + stepMs - 1;
            windows.add(new long[]{cursor, lastOpenMsInWindow, endMsParam});
            cursor = lastOpenMsInWindow + stepMs;
        }
    
        int rps = Math.max(1, config.getRequestsPerSecond());
        int maxParallel = Math.min(16, Math.max(2, rps)); // 可按需调整

        // 使用独立的窗口线程池，避免与缺口级并发池相互阻塞
        ExecutorCompletionService<WindowResult> ecs =
                new ExecutorCompletionService<>(windowExecutor);

        int submitted = 0;
        int completed = 0;
        int desired = routeReady.contains(marketType.toLowerCase()) ? maxParallel : 1;
    
        // 预热：填满期望并发
        while (submitted < windows.size() && (submitted - completed) < desired) {
            long[] w = windows.get(submitted++);
            String url = buildRestUrl(symbol, marketType, period, w[0], w[2], apiMaxLimit);
            ecs.submit(() -> {
                try {
                    List<Kline> result = fetchKlines(url, marketType);
                    return new WindowResult(w, result);
                } catch (Exception ex) {
                    log.warn("窗口拉取失败 {} {} {} [{} - {}]: {}", symbol, marketType, period, w[0], w[1], ex.getMessage());
                    return new WindowResult(w, Collections.emptyList());
                }
            });
        }
    
        // 完成即处理 + 先补满并发后再缓冲写库
        while (completed < windows.size()) {
            WindowResult wr = ecs.take().get();  // 注意：wr 作用域仅在此循环体内
            completed++;
    
            // 路由就绪后提升并发
            desired = routeReady.contains(marketType.toLowerCase()) ? maxParallel : 1;
    
            // 先补满目标并发，保持 REST 并发稳定
            while (submitted < windows.size() && (submitted - completed) < desired) {
                long[] w = windows.get(submitted++);
                String url = buildRestUrl(symbol, marketType, period, w[0], w[2], apiMaxLimit);
                ecs.submit(() -> {
                    try {
                        List<Kline> result = fetchKlines(url, marketType);
                        return new WindowResult(w, result);
                    } catch (Exception ex) {
                        log.warn("窗口拉取失败 {} {} {} [{} - {}]: {}", symbol, marketType, period, w[0], w[1], ex.getMessage());
                        return new WindowResult(w, Collections.emptyList());
                    }
                });
            }
    
            // 再缓冲当前窗口结果到 DataBufferService（异步批量写库）
            if (!wr.klines.isEmpty()) {
                dataBufferService.bufferKlines(symbol, marketType, period, wr.klines);
            }
        }
    }

    private List<Kline> fetchKlines(String url, String marketType) throws Exception {
        apiRateLimiter.acquire();
        log.info("拉取REST数据: {} {}", marketType, url);
        
        try {
            Response resp = executeWithRetries(url, marketType);
            if (resp == null) {
                log.warn("REST响应为空(重试耗尽): {} {}", marketType, url);
                return Collections.emptyList();
            }
            try (resp) {
                apiRateLimiter.afterResponse(resp);
                int code = resp.code();
                if (!resp.isSuccessful()) {
                    if (code == 403 || code == 451) {
                        log.info("REST响应{}，已启用代理: {} -> {}", code, marketType, url);
                        enableProxyForMarketType(marketType);
                    }
                    log.warn("REST非成功响应: code={} url={}", code, url);
                    return Collections.emptyList();
                }
                // 标记该 marketType 路由已就绪（后续窗口提升并发）
                routeReady.add(marketType.toLowerCase());
    
                String body = resp.body() != null ? resp.body().string() : "[]";
                if (body.isEmpty() || "[]".equals(body)) {
                    log.warn("REST响应体为空或空数组: url={}", url);
                    return Collections.emptyList();
                }
                
                JsonNode root = objectMapper.readTree(body);
                if (!root.isArray()) {
                    log.warn("REST负载不是数组: code={} url={} body={}", code, url, body.length() > 200 ? body.substring(0, 200) + "..." : body);
                    return Collections.emptyList();
                }
                List<Kline> out = new ArrayList<>();
                for (JsonNode n : root) {
                    if (!n.isArray() || n.size() < 11) continue;
                    long openTime = n.get(0).asLong(0);
                    long closeTime = n.get(6).asLong(0);
                    BigDecimal open = new BigDecimal(n.get(1).asText("0"));
                    BigDecimal high = new BigDecimal(n.get(2).asText("0"));
                    BigDecimal low = new BigDecimal(n.get(3).asText("0"));
                    BigDecimal close = new BigDecimal(n.get(4).asText("0"));
                    BigDecimal volume = new BigDecimal(n.get(5).asText("0"));
                    BigDecimal quoteVolume = new BigDecimal(n.get(7).asText("0"));
                    long count = n.get(8).asLong(0);
                    BigDecimal takerBuyBaseVolume = new BigDecimal(n.get(9).asText("0"));
                    BigDecimal takerBuyQuoteVolume = new BigDecimal(n.get(10).asText("0"));

                    Kline k = Kline.builder()
                            .openTime(openTime)
                            .closeTime(closeTime)
                            .open(open)
                            .high(high)
                            .low(low)
                            .close(close)
                            .volume(volume)
                            .quoteVolume(quoteVolume)
                            .count(count)
                            .takerBuyBaseVolume(takerBuyBaseVolume)
                            .takerBuyQuoteVolume(takerBuyQuoteVolume)
                            .build();

                    if (k.isValid()) {
                        out.add(k);
                    }
                }
                return out;
            }
        } catch (Exception e) {
            log.error("fetchKlines执行异常: url={} error={}", url, e.getMessage(), e);
            throw e;
        }
    }

    private Response executeWithRetries(String url, String marketType) {
        int attempts = 0;
        while (attempts < restRetries) {
            OkHttpClient client = selectClient(marketType);
            Request req = new Request.Builder().url(url).get().build();
            try {
                Response resp = client.newCall(req).execute();
                int code = resp.code();
    
                if (code == 403 || code == 451) {
                    enableProxyForMarketType(marketType);
                    log.info("收到 {}，切换至代理后重试: {}", code, url);
                    resp.close();
                    attempts++;
                    sleep(500L);
                    continue;
                }
    
                if (code == 429 && attempts < restRetries - 1) {
                    String retry = resp.header("Retry-After");
                    long retryMs = retry != null ? Long.parseLong(retry) * 1000L : 1000L;
                    log.info("收到429限流，等待 {}ms 后重试: {}", retryMs, url);
                    sleep(retryMs);
                    resp.close();
                    attempts++;
                    continue;
                }
    
                if (resp.isSuccessful() || attempts == restRetries - 1) {
                    return resp;
                }
                
                log.warn("REST请求失败 code={}, 准备重试: {}", code, url);
                resp.close();
            } catch (Exception e) {
                log.warn("REST request failed (attempt {}/{}): {} -> {}", attempts + 1, restRetries, url, e.getMessage());
                if (attempts == restRetries - 1) {
                    log.error("REST请求最终失败，所有重试已耗尽: {}", url, e);
                    return null;
                }
            }
            attempts++;
            long backoffMs = 1000L * (1L << attempts); // exponential backoff
            log.info("指数退避等待 {}ms 后重试", backoffMs);
            sleep(backoffMs);
        }
        return null;
    }

    private OkHttpClient selectClient(String marketType) {
        boolean useProxy = marketTypesUsingProxy.contains(marketType.toLowerCase());
        if (useProxy && proxyClient != null) return proxyClient;
        if (useProxy && proxyClient == null) {
            log.info("已标记使用代理但未配置代理客户端: marketType={}", marketType);
        }
        return defaultClient;
    }

    // 替换原来的 buildSocksClient 为 HTTP 代理实现
    private OkHttpClient buildHttpProxyClient(ProxyConfig cfg) {
        Proxy httpProxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(cfg.getHost(), cfg.getPort()));

        // OkHttp 的代理认证：收到 407 时添加 Proxy-Authorization
        Authenticator proxyAuth = (route, response) -> {
            // 防止认证死循环
            if (response.request().header("Proxy-Authorization") != null) {
                return null;
            }
            String credential = Credentials.basic(cfg.getUsername(), cfg.getPassword());
            return response.request().newBuilder()
                    .header("Proxy-Authorization", credential)
                    .build();
        };

        return new OkHttpClient.Builder()
                .proxy(httpProxy)
                .proxyAuthenticator(proxyAuth)
                .connectTimeout(20, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .callTimeout(120, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .addInterceptor(chain -> {
                    Request originalRequest = chain.request();
                    Request newRequest = originalRequest.newBuilder()
                            .header("Accept-Encoding", "identity")
                            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
                            .build();
                    return chain.proceed(newRequest);
                })
                .build();
    }

    private void enableProxyForMarketType(String marketType) {
        marketType = marketType.toLowerCase();
        if (!marketTypesUsingProxy.contains(marketType)) {
            marketTypesUsingProxy.add(marketType);
            log.warn("启用代理（本次运行内永久化） marketType={}", marketType);
            if (proxyClient == null) {
                try {
                    Optional<ProxyConfig> pc = databaseService.getProxyConfig();
                    if (pc.isPresent()) {
                        ProxyConfig cfg = pc.get();
                        proxyClient = buildHttpProxyClient(cfg);
                        log.info("动态加载代理配置: {}:{} (HTTP代理)", cfg.getHost(), cfg.getPort());
                    } else {
                        log.info("未找到 proxy_config 配置，仍将使用直连客户端。");
                    }
                } catch (Exception e) {
                    log.warn("动态加载代理配置失败", e);
                }
            }
        }
    }

    // ========== Helpers ==========

    private long getPeriodStepSeconds(String period) {
        switch (period.toLowerCase()) {
            case "1m": return 60L;
            case "1h": return 3600L;
            default: throw new IllegalArgumentException("Unsupported period: " + period);
        }
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }

    private String buildRestUrl(String symbol, String marketType, String period, long startMs, long endMs, int limit) {
        symbol = symbol.toUpperCase();
        marketType = marketType.toLowerCase();
        period = period.toLowerCase();

        // 基础 URL 由 yml 控制（ApplicationConfig.getRestApiBaseUrl）
        String base = config.getRestApiBaseUrl(marketType);

        HttpUrl url = HttpUrl.parse(base).newBuilder()
                .addQueryParameter("symbol", symbol)
                .addQueryParameter("interval", period)
                .addQueryParameter("startTime", String.valueOf(startMs))
                .addQueryParameter("endTime", String.valueOf(endMs))
                .addQueryParameter("limit", String.valueOf(limit))
                .build();
        return url.toString();
    }

    // 步长对齐到最近过去的边界
    private long alignToStep(long t, long stepMs) {
        return t - (t % stepMs);
    }
}