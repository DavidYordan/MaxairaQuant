# QuantService 部署规范（Ubuntu 24 + 宝塔）

目标
- 在 Ubuntu 24 上以守护进程方式部署运行，托管于 systemd 或宝塔的进程管理插件，保证稳定、可恢复。
- 打包为单文件可执行（推荐 Nuitka），或使用 PyInstaller+PyArmor 进行代码混淆与便捷加密，降低逆向成本。
- 严格遵循 service_bak_core.md 的数据源、回填、WS 与代理策略。

一、环境准备
- 系统组件
  - Python 3.11（或 3.10/3.12，需保持依赖兼容）
  - ClickHouse Server（或远程），开放 `http_port=8123`
  - NTP/时间同步：启用 `systemd-timesyncd`，保证时钟准确
- 必备依赖（构建阶段）
  - build-essential、python3-dev、libffi-dev、libssl-dev、pkg-config
- 安装命令（需要 sudo）
  - apt 更新与工具安装
  - Python 与 pip 安装（如未安装）

二、构建产物（二选一）
- A. Nuitka（推荐，生成单文件 ELF）
  - 安装 Nuitka 与依赖
  - 打包命令：将 `QuantService/main.py` 打包，并携带 `config/app.yaml`
  - 产物：`dist/main.bin`（名称示例）
- B. PyInstaller + PyArmor（备选）
  - 使用 PyInstaller 生成可执行；用 PyArmor 对源码进行混淆/加壳
  - 优点：操作简单；缺点：抗逆向性弱于 Nuitka

三、部署目录布局
- `/opt/quantservice/`
  - `bin/quantservice`（二进制/可执行）
  - `config/app.yaml`（只读，外置）
  - `logs/`（运行日志）
- 权限与用户
  - 系统用户：`quantsvc`（非 root 运行）
  - 目录权限：`chown -R quantsvc:quantsvc /opt/quantservice`；`chmod -R 750 /opt/quantservice`

四、运行与守护
- 方式一：systemd
  - 在 `/etc/systemd/system/quantservice.service` 定义服务单元
  - `ExecStart` 指定可执行与配置路径（建议设置 `QS_CONFIG_PATH` 环境变量）
  - 启动与开机自启：`systemctl daemon-reload`、`systemctl enable --now quantservice`
- 方式二：宝塔面板（Supervisor 插件）
  - 添加新程序，命令路径指向 `/opt/quantservice/bin/quantservice`
  - 配置工作目录与环境变量（`QS_CONFIG_PATH=/opt/quantservice/config/app.yaml`）
  - 设置自动重启策略

五、配置管理
- `config/app.yaml` 与项目中一致，注意：
  - REST 端点：`data-api.binance.vision`（Spot），`fapi.binance.com`（UM），`dapi.binance.com`（CM）
  - WS 端点：`wss://stream.binance.com:9443/ws/`、`wss://fstream.binance.com/ws/`、`wss://dstream.binance.com/ws/`
  - 回填起始：`2020-01-01`（按 service_bak_core.md）
- 代理：SOCKS5（REST 专用）
  - 在 `proxy_config` 写入 `host/port/username/password` 与 `enabled=1`

六、便捷加密/混淆（增强逆向成本）
- Nuitka：编译为 ELF 单文件，源码不会直接暴露；建议搭配最小符号信息
- PyArmor：混淆与加壳；建议与 PyInstaller 配合
- 额外措施
  - 将敏感配置（如代理凭据）放入 ClickHouse 表而非本地文件
  - 外置配置文件设置为 640 权限，仅 `quantsvc` 可读
  - 不在日志中输出密钥类敏感字段
- 风险提示：上述方案属于“提高逆向成本”，并不提供强安全保证

七、升级与回滚
- 升级：停服、替换 `bin/quantservice`、校验配置、启服
- 回滚：保留上一个版本的 `bin/quantservice.bak`，替换并重启
- 日志与数据：日志滚动保留 7 天；数据由 ClickHouse 管理，无需迁移

八、监控与故障处理
- 指标日志：每 60s 输出回填路径的 `req/ok/fail/rows`
- 常见故障
  - REST 403/451：检查 `proxy_config` 与网络；代理切换后再试
  - 429：观察 `X-MBX-USED-WEIGHT-1M`，等待下一分钟
  - WS 连接失败（美国服务器限制）：视情况改用 REST 近实时方案