# QuantService 部署规范（Ubuntu 24 + 宝塔）

## 项目概述

QuantService 是一个基于 Python 的量化交易数据服务系统，主要功能包括：
- **数据回填服务**：从 Binance 获取历史 K 线数据并存储到 ClickHouse
- **实时数据流**：通过 WebSocket 获取实时 K 线数据
- **指标计算**：在线和离线技术指标计算服务
- **回测引擎**：策略回测和性能评估
- **交易服务**：订单管理和风控系统
- **代理管理**：SOCKS5 代理轮换和管理
- **缺口修复**：自动检测和修复数据缺口

## 一、环境准备

### 系统要求
- **操作系统**：Ubuntu 24.04 LTS
- **Python 版本**：3.11+ （推荐 3.11，兼容 3.10/3.12）
- **数据库**：ClickHouse Server 23.0+
- **内存**：最小 4GB，推荐 8GB+
- **存储**：SSD 推荐，至少 100GB 可用空间

### 系统依赖安装
```bash
# 更新系统包
sudo apt update && sudo apt upgrade -y

# 安装构建工具和依赖
sudo apt install -y build-essential python3-dev python3-pip python3-venv
sudo apt install -y libffi-dev libssl-dev pkg-config git curl

# 安装 ClickHouse（如果本地部署）
curl https://clickhouse.com/ | sh
sudo ./clickhouse install

# 启用时间同步
sudo systemctl enable --now systemd-timesyncd
```

### Python 环境配置
```bash
# 创建虚拟环境（构建阶段）
python3 -m venv /opt/quantservice-build
source /opt/quantservice-build/bin/activate

# 安装项目依赖
pip install -r requirements.txt
```

## 二、构建与打包

### 方式 A：Nuitka 打包（推荐）
```bash
# 安装 Nuitka
pip install nuitka

# 打包命令（基于 develop.md）
python -m nuitka \
    --lto=yes \
    --follow-imports \
    --include-data-files=config/app.yaml=config/app.yaml \
    --output-dir=dist \
    --onefile \
    --output-filename=quantservice \
    main.py

# 产物位置：dist/quantservice
```

### 方式 B：PyInstaller + PyArmor（备选）
```bash
# 安装工具
pip install pyinstaller pyarmor

# PyArmor 混淆（可选）
pyarmor gen --recursive --output obfuscated/ qs/

# PyInstaller 打包
pyinstaller --onefile --name quantservice \
    --add-data "config/app.yaml:config" \
    main.py
```

## 三、部署目录结构

```
/opt/quantservice/
├── bin/
│   ├── quantservice          # 主程序可执行文件
│   └── quantservice.bak      # 备份版本（升级时保留）
├── config/
│   ├── app.yaml             # 主配置文件
│   └── app.yaml.bak         # 配置备份
├── logs/                    # 日志目录
│   ├── quantservice.log
│   └── archived/            # 历史日志
└── data/                    # 临时数据目录（可选）
```

### 权限配置
```bash
# 创建系统用户
sudo useradd -r -s /bin/false -d /opt/quantservice quantsvc

# 设置目录权限
sudo mkdir -p /opt/quantservice/{bin,config,logs,data}
sudo chown -R quantsvc:quantsvc /opt/quantservice
sudo chmod -R 750 /opt/quantservice
sudo chmod 640 /opt/quantservice/config/app.yaml
```

## 四、配置文件

### 主配置文件 `/opt/quantservice/config/app.yaml`
```yaml
app:
  environment: prod

  clickhouse:
    host: "localhost"          # ClickHouse 服务器地址
    port: 8123
    database: "maxaira"
    username: "default"
    password: ""               # 生产环境建议设置密码

  binance:
    spot:
      ws_url: "wss://stream.binance.com:9443/ws/"
      rest_url: "https://data-api.binance.vision/api/v3/klines"
    um:
      ws_url: "wss://fstream.binance.com/ws/"
      rest_url: "https://fapi.binance.com/fapi/v1/klines"
    cm:
      ws_url: "wss://dstream.binance.com/ws/"
      rest_url: "https://dapi.binance.com/dapi/v1/klines"
```

## 五、服务管理

### 方式一：systemd 服务
创建服务文件 `/etc/systemd/system/quantservice.service`：
```ini
[Unit]
Description=QuantService - Quantitative Trading Data Service
After=network.target clickhouse-server.service
Wants=clickhouse-server.service

[Service]
Type=simple
User=quantsvc
Group=quantsvc
WorkingDirectory=/opt/quantservice
ExecStart=/opt/quantservice/bin/quantservice
Environment=QS_CONFIG_PATH=/opt/quantservice/config/app.yaml
Environment=PYTHONUNBUFFERED=1

# 重启策略
Restart=always
RestartSec=10
StartLimitInterval=60
StartLimitBurst=3

# 资源限制
LimitNOFILE=65536
LimitNPROC=4096

# 日志配置
StandardOutput=journal
StandardError=journal
SyslogIdentifier=quantservice

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable quantservice
sudo systemctl start quantservice
sudo systemctl status quantservice
```

### 方式二：宝塔面板 Supervisor
在宝塔面板的 Supervisor 管理器中添加：
- **名称**：quantservice
- **启动用户**：quantsvc
- **运行目录**：/opt/quantservice
- **启动命令**：/opt/quantservice/bin/quantservice
- **环境变量**：
  ```
  QS_CONFIG_PATH=/opt/quantservice/config/app.yaml
  PYTHONUNBUFFERED=1
  ```

## 六、代理配置

### SOCKS5 代理设置
代理配置存储在 ClickHouse 中，通过以下 SQL 管理：

```sql
-- 创建代理配置表（系统自动创建）
-- 添加代理配置
INSERT INTO proxy_config (host, port, username, password, enabled, created_at)
VALUES ('proxy.example.com', 1080, 'user', 'pass', 1, now());

-- 查看当前启用的代理
SELECT * FROM proxy_config WHERE enabled = 1 ORDER BY created_at DESC LIMIT 1;
```

### 代理轮换策略
- 系统自动检测代理可用性
- REST API 请求失败时自动切换代理
- WebSocket 连接不使用代理（直连）

## 七、数据库初始化

### ClickHouse 表结构
系统启动时会自动创建以下表：
- **K线数据表**：`klines_{symbol}_{market}_{period}`
- **交易对配置**：`trading_pairs`
- **代理配置**：`proxy_config`
- **指标数据表**：`indicators_*`
- **回测结果**：`backtest_results`

### 手动初始化（如需要）
```sql
-- 连接 ClickHouse
clickhouse-client

-- 创建数据库
CREATE DATABASE IF NOT EXISTS maxaira;
USE maxaira;

-- 系统会自动创建其他表
```

## 八、服务架构说明

### 核心服务模块
1. **BackfillManager**：历史数据回填服务
   - 支持多市场（现货、U本位、币本位）
   - 并发窗口回填，提高效率
   - 自动重试和错误处理

2. **WebSocketSupervisor**：实时数据流管理
   - 自动重连机制
   - 心跳检测和超时处理
   - 多流并发管理

3. **GapHealScheduler**：数据缺口修复
   - 定期扫描数据缺口
   - 自动触发回填任务
   - 1分钟数据每30秒检查，1小时数据每5分钟检查

4. **IndicatorService**：技术指标计算
   - 在线实时计算
   - 离线批量计算
   - 支持多种技术指标

5. **ClientServer**：客户端数据下发
   - WebSocket 服务端
   - 实时数据推送
   - 客户端连接管理

### CLI 工具使用
```bash
# 手动回填数据缺口
./quantservice backfill-gap --symbol ETHUSDT --market um --period 1m --start_ms 1640995200000 --end_ms 1641081600000

# 启动独立的客户端服务
./quantservice clientserver --host 0.0.0.0 --port 8765

# 启动临时 WebSocket 流
./quantservice ws-start --symbol BTCUSDT --market spot --period 1m
```

## 九、监控与日志

### 日志配置
- 使用 loguru 进行日志管理
- 日志级别：INFO（生产环境）
- 日志轮转：按大小和时间自动轮转
- 保留策略：7天内的日志

### 监控指标
系统每60秒输出关键指标：
```
Metrics: req=1250 ok=1248 fail=2 rows=124800
```
- **req**：总请求数
- **ok**：成功请求数
- **fail**：失败请求数
- **rows**：插入的数据行数

### 健康检查
```bash
# 检查服务状态
sudo systemctl status quantservice

# 查看实时日志
sudo journalctl -u quantservice -f

# 检查数据库连接
clickhouse-client -q "SELECT count() FROM maxaira.trading_pairs"
```

## 十、故障处理

### 常见问题及解决方案

1. **REST API 403/451 错误**
   - 检查代理配置和网络连接
   - 切换到其他可用代理
   - 确认 Binance API 访问权限

2. **429 限流错误**
   - 观察 `X-MBX-USED-WEIGHT-1M` 响应头
   - 降低请求频率
   - 等待下一分钟窗口重置

3. **WebSocket 连接失败**
   - 检查网络连接和防火墙设置
   - 美国服务器可能被限制，考虑使用代理
   - 必要时切换到 REST 近实时方案

4. **ClickHouse 连接问题**
   - 检查 ClickHouse 服务状态
   - 验证连接配置和权限
   - 检查网络连通性

5. **内存使用过高**
   - 调整 buffer 配置中的 batch_size
   - 减少并发窗口数量
   - 检查是否有内存泄漏

## 十一、升级与维护

### 升级流程
```bash
# 1. 停止服务
sudo systemctl stop quantservice

# 2. 备份当前版本
sudo cp /opt/quantservice/bin/quantservice /opt/quantservice/bin/quantservice.bak
sudo cp /opt/quantservice/config/app.yaml /opt/quantservice/config/app.yaml.bak

# 3. 部署新版本
sudo cp new_quantservice /opt/quantservice/bin/quantservice
sudo chown quantsvc:quantsvc /opt/quantservice/bin/quantservice
sudo chmod 750 /opt/quantservice/bin/quantservice

# 4. 验证配置
sudo -u quantsvc /opt/quantservice/bin/quantservice --check-config

# 5. 启动服务
sudo systemctl start quantservice
sudo systemctl status quantservice
```

### 回滚流程
```bash
# 快速回滚到上一版本
sudo systemctl stop quantservice
sudo cp /opt/quantservice/bin/quantservice.bak /opt/quantservice/bin/quantservice
sudo systemctl start quantservice
```

### 定期维护
- **日志清理**：定期清理过期日志文件
- **数据库优化**：定期执行 ClickHouse OPTIMIZE 操作
- **配置备份**：定期备份配置文件和数据库
- **性能监控**：监控系统资源使用情况

## 十二、安全加固

### 代码保护
- **Nuitka 编译**：源码编译为二进制，提高逆向难度
- **符号剥离**：移除调试符号信息
- **配置加密**：敏感配置存储在数据库中

### 系统安全
- **用户隔离**：使用专用系统用户运行服务
- **文件权限**：严格控制文件和目录权限
- **网络安全**：配置防火墙规则，限制不必要的网络访问
- **日志安全**：避免在日志中输出敏感信息

### 监控告警
- **异常检测**：监控异常错误和性能指标
- **资源监控**：CPU、内存、磁盘使用率监控
- **业务监控**：数据完整性和服务可用性监控

---

**注意事项**：
1. 生产环境部署前请充分测试所有功能
2. 建议使用配置管理工具管理多环境配置
3. 定期更新依赖包和系统补丁
4. 建立完善的备份和恢复机制