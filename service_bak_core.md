# service_bak 数据采集后端核心笔记（用于迁移）

本笔记聚焦三项核心：数据库结构、REST 历史补采、WS 实时采集，以及美国服务器场景下的代理策略，确保迁移到 Python 前对现状有清晰、可回溯的记录。

## 范围与目的
- 项目：`service_bak/data-collector`（Java 21，Spring Boot 3.5）
- 数据源：Binance（Spot、USDT 永续 UM、币本位永续 CM）
- 存储：ClickHouse（JDBC + HikariCP）
- 目标：记录当前可运行 Java 后端的实际行为与配置，降低迁移调试成本

## 部署与运行关键点
- 服务端口：`server.port=8080`
- Actuator：`/actuator/health`、`/actuator/metrics`、`/actuator/info`
- 重要配置文件：`src/main/resources/application.yml`
- 入口类：`com.maxaira.DataCollectorApplication`（启用定时任务与异步）

## 数据库结构（ClickHouse）
- 数据库连接（yml）
  - `app.clickhouse.host`, `port`, `database`, `username`, `password`
  - DataSource 构建：`ApplicationConfig.clickHouseDataSource()`（HikariCP）
- 表清单（应用启动时初始化，`DatabaseService.run()`）
  - `trading_pair_config`（交易对配置）
    - 字段：`id UInt64`, `symbol String`, `market_type String`, `enabled UInt8`, `created_at DateTime`, `updated_at DateTime`
    - 引擎：`MergeTree()`，`ORDER BY (symbol, market_type)`
    - 初始化：从 `app.database.main-stream-symbols` 与 `market-types` 批量插入；仅 `ETHUSDT` 的 `um` 默认启用
  - `proxy_config`（代理配置）
    - 字段：`id UInt64`, `host String`, `port UInt16`, `username String`, `password String`, `enabled UInt8`, 时间戳字段同上
    - 引擎：`MergeTree()`，`ORDER BY (host, port)`
    - 用途：REST 403/451 时切换到此代理（HTTP 代理 + 认证）
  - K 线分表（固定周期 1m、1h）
    - 命名：`kline_{symbol}_{marketType}_{period}`，例如 `kline_ethusdt_um_1m`
    - 字段：
      - `open_time UInt64`, `close_time UInt64`
      - `open/high/low/close Decimal128(8)`
      - `volume/quote_volume Decimal128(8)`
      - `count UInt64`
      - `taker_buy_base_volume/taker_buy_quote_volume Decimal128(8)`
      - `version UInt64 DEFAULT toUInt64(toUnixTimestamp(now()))`
    - 引擎/分区：`ReplacingMergeTree(version)`，`PARTITION BY toYYYYMM(toDateTime(open_time/1000))`，`ORDER BY open_time`
    - 去重策略：批内按 `open_time` 去重 + ReplacingMergeTree 最终版本覆盖
- 关键工具方法
  - 表名生成：`ApplicationConfig.getTableName(symbol, marketType, period)`
  - 最大时间查询：`DatabaseService.getMaxOpenTime(table)`
  - 缺口检测：`DatabaseService.findGapsWindowed(table, start, end, step)`

## REST 历史补采（Backfill）
- 周期与步长
  - 支持周期：`1m`（60s）、`1h`（3600s）
  - 步长：`stepMs = periodSeconds * 1000`
- 起始时间（High Water Mark）
  - 内存注册表：`HighWaterMarkRegistry`（非持久化）
  - 初始值：`app.database.backfill-start-date`（默认 `2020-01-01` 0 点 UTC）
- 缺口发现（ClickHouse + SQL 窗口函数）
  - 使用 `lag(open_time) OVER (ORDER BY open_time)` 检测相邻时间差 > 步长
  - 首尾边界处理：区间内 `min(open_time)` 与 `max(open_time)`，构造前导/尾随缺口
  - 空区间：直接认定 `[startOpenMs, endOpenMs]` 为整段缺口
- REST URL 构造（`BackfillService.buildRestUrl()`）
  - Spot（美国常见屏蔽 `api.binance.com`，改用镜像）：
    - `https://data-api.binance.vision/api/v3/klines`
  - UM（USDT 永续）：`https://fapi.binance.com/fapi/v1/klines`
  - CM（币本位永续）：`https://dapi.binance.com/dapi/v1/klines`
  - 参数：`symbol`, `interval`, `startTime`, `endTime`, `limit`（`app.binance.api-max-limit`，默认 1000）
- 窗口并发与背压（每缺口拆分多窗口）
  - 窗口大小：`windowMs = stepMs * limit`
  - 并发调度：
    - 缺口级并发池：`backfillExecutor`
    - 窗口级并发池：`windowExecutor`（`ExecutorCompletionService` 保持稳定并发）
    - `routeReady`：首次成功后将 `marketType` 标记为“路由就绪”，提升到 `maxParallel`
- 速率限制（`ApiRateLimiter`）
  - 每秒许可：`app.binance.requests-per-second`（默认 15）
  - 分钟权重：读取响应头 `X-MBX-USED-WEIGHT-1M`，临界时阻塞到下一分钟
  - 429 处理：读 `Retry-After` 秒数并指数退避
- 代理切换（HTTP Proxy + 认证）
  - 触发条件：REST 返回 403 或 451（合规限制）
  - 行为：
    - 将 `marketType` 加入 `marketTypesUsingProxy`（本次进程内生效）
    - 从 `proxy_config` 读取首个启用项，构建 `OkHttpClient`：
      - `client.proxy(new Proxy(HTTP, host:port))`
      - `client.proxyAuthenticator((route, response) -> header("Proxy-Authorization", basic(username,password)))`
    - 代理仅应用于 REST；WS 未配置代理
- 负载解析与写库
  - JSON 数组（长度 ≥ 11）解析为 `Kline` 对象，校验 `isValid()`
  - 缓冲写库：`DataBufferService.bufferKlines(...)`（Disruptor + 批量插入）

## WS 实时采集（WebSocket）
- 上游 WS 端点（`ApplicationConfig.getWebSocketBaseUrl`）
  - Spot：`wss://stream.binance.com:9443/ws/`
  - UM：`wss://fstream.binance.com/ws/`
  - CM：`wss://dstream.binance.com/ws/`
- 订阅流：`{symbol}@kline_{period}`（小写）
- 连接管理（`WebSocketService`）
  - 心跳：定时检查 `lastMessageMs` 超过 `app.websocket.heartbeat-timeout` 则重连
  - 重连：指数退避（上限 `app.websocket.max-reconnect-attempts`）
  - 并发线程池：OkHttp WebSocket 专用线程 + 调度器（心跳）
- 消息处理
  - 先原样广播给客户端（含未收盘 tick）：`ClientBroadcastService.broadcastRaw(...)`
  - 仅在 `k.x == true`（已收盘）时写入：提取 `k` 字段生成 `Kline` 并 `bufferKline(...)`
- 重要提示（美国服务器）
  - WS 没有代理配置；如 `fstream/dstream` 在美国服务器受限，实时可能失败
  - 可通过 `GET /api/websocket/status` 观察连接数与最近心跳

## 客户端广播与控制接口
- 客户端 WS（供前端订阅渲染）：`/ws/stream`
  - 消息格式（入站 JSON）：
    - `{"action":"subscribe","symbol":"ETHUSDT","market":"um","period":"1m"}`
    - `{"action":"unsubscribe","symbol":"ETHUSDT","market":"um","period":"1m"}`
    - `{"action":"ping"}`
  - 服务端行为：
    - `subscribe`：加入订阅并确保上游 WS 已启动
    - 广播：Java 后端不加工原始文本，客户端自行解析
- REST 控制器
  - 交易对配置
    - `GET /api/config`：获取启用配置
    - `GET /api/config/all`：获取所有配置
    - `POST /api/config?symbol=XXX&marketType=YYY`：新增（默认禁用）
    - `POST /api/config/{id}/toggle?enabled=true|false`：
      - `enabled=true`：启动 1m/1h WS 并提交回填任务
      - `enabled=false`：停止 WS（历史数据保留）
  - 状态查询
    - `GET /api/websocket/status`：当前 WS 流列表与心跳
    - `GET /api/buffer/status`：缓冲队列、批次、TPS、成功率等

## 配置与参数来源（application.yml）
- `app.binance`：
  - `data-download-base-url`: `https://data.binance.vision`
  - `spot-ws-url`, `um-ws-url`, `cm-ws-url`
  - `spot-rest-url`, `um-rest-url`, `cm-rest-url`
  - `api-max-limit`（默认 1000）、`requests-per-second`（默认 15）
- `app.clickhouse`：连接信息（`host/port/database/username/password`）
- `app.database`：
  - `backfill-start-date`（例如 `2020-01-01`）
  - `main-stream-symbols`（默认含 `BTCUSDT/ETHUSDT`）
  - `market-types`（默认含 `SPOT/UM/CM`）
- `app.data-buffer`：缓冲与批量写库参数（大小/超时/策略）
- `app.websocket`：心跳与重连策略
- `app.monitoring`：性能日志与指标开关

## 监控与日志
- Actuator 暴露 `/actuator` 指标；日志级别在 `application.yml/logback-spring.xml` 配置
- `DataBufferService` 周期性输出性能指标：`TPS/成功率/批次/缓冲区数`

## 美国服务器实际运行策略与风险点
- Spot REST：使用镜像域名 `data-api.binance.vision`，通常无需代理
- UM/CM REST：使用 `fapi.binance.com` / `dapi.binance.com`
  - 若返回 `403/451`，自动切换到 `proxy_config` 的 HTTP 代理（需填 `host/port/username/password`，且 `enabled=1`）
- WS（spot/um/cm）：未配置代理
  - 美国服务器可能限制 `fstream/dstream`，导致 WS 连接失败或频繁重连
  - 迁移到 Python 时需考虑 WS 代理支持或改为 REST 驱动的近实时方案
- 缺口修复：`GapHealScheduler` 定时调度（30 秒与 5 分钟间隔）提交 1m/1h 回填任务

## 迁移注意事项与建议
- 保持表结构与命名一致（`kline_{symbol}_{market}_{period}`，字段与类型对应）
- 延续缺口检测逻辑（`lag` + 首尾边界），确保回填幂等与完整
- 延续 REST 代理策略：首次 `403/451` 切换到代理（支持 `Proxy-Authorization`）
- 保留 `requests-per-second` 和 `X-MBX-USED-WEIGHT-1M` 读头逻辑，避免分钟权重爆表
- WS 只在 `k.x==true` 时写入收盘 K 线；未收盘广播给前端用于展示
- 观察性：提供健康检查、缓冲/WS 状态与性能日志，复用现有接口契约

## 验证清单（上线前）
- ClickHouse：
  - `SELECT COUNT(*) FROM trading_pair_config WHERE enabled=1`
  - `SELECT * FROM proxy_config WHERE enabled=1 LIMIT 1`
  - 任一 K 线表近一小时 `open_time` 连续性检查
- REST：
  - Spot 无代理拉取一段窗口（镜像域）
  - UM/CM 在美国环境触发 403/451 后确认代理切换与成功响应
- WS：
  - Spot/UM/CM：在美国服务器上尝试连通；如受限，记录失败与重连日志
- 缓冲与写库：
  - 批量插入与去重生效；性能日志存在且数值合理（TPS/成功率）

——
文件来源与参考类：
- 配置：`src/main/resources/application.yml`
- 数据源与工具：`com.maxaira.config.ApplicationConfig`
- 数据库：`com.maxaira.service.DatabaseService`
- 回填：`com.maxaira.service.BackfillService`、`BackfillTaskManager`、`HighWaterMarkRegistry`
- 速率限制：`com.maxaira.service.ApiRateLimiter`
- 缓冲：`com.maxaira.service.DataBufferService`
- WS：`com.maxaira.service.WebSocketService`、`com.maxaira.websocket.*`
- 控制器：`com.maxaira.controller.*`