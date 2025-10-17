项目名称：QuantStudio - 高性能量化交易终端（2025年10月Win10 桌面版）
一、项目背景与目标

当前市面上多图回测和量化终端大多偏重图表或策略脚本，对高频行情下的实时多屏指标计算与低延迟渲染支持有限。
本项目旨在开发一款基于 C++/DirectX11/ImGui/ImPlot 的高性能桌面软件，用于：

同时监控多时间周期（例如 1m、5m、20m、1h）行情；

实时计算并展示数十种技术指标；

支撑本地策略运行与回测；

后续可扩展为直接交易 Binance Futures（实盘/仿真）。

二、总体架构
1. 技术栈
层级	技术/框架	说明
GUI 层	Dear ImGui (docking 分支) + ImPlot	高性能即时模式 UI + 实时绘图
渲染层	DirectX 11	原生 GPU 渲染接口
网络层	Asio / Boost.Asio	异步 WebSocket + REST 客户端
指标引擎	TA-Lib + Tulip Indicators (C)	计算 MA/EMA/RSI/BB 等指标
数据模型	自研 SoA（结构化数组）	高效时间序列缓存
并发框架	moodycamel::ConcurrentQueue 或 atomic_queue	多线程无锁通信
持久化	本地二进制 / CSV / Parquet	历史数据缓存与回放
配置	JSON / YAML	系统与策略配置文件
三、功能需求
1. 主界面与多屏布局

支持 2×2 多视图布局（Docking，可自由拖拽组合）；

每个视图可单独设置：

时间周期（1m / 5m / 20m / 1h 等）；

显示指标（如 MA、EMA、RSI、BOLL、SAR、VOL 等）；

数据源（实时 / 回放）。

支持界面布局保存、加载。

2. 行情数据

接入 Binance Futures 实时数据：

合并流 WebSocket：ethusdt@kline_*、@aggTrade、@markPrice；

REST：用于历史K线补齐；

心跳、断线重连、ListenKey 维护。

本地缓存与回放：

支持 CSV 或二进制格式；

可调速播放（1×、2×、10×）。

3. 指标计算引擎

支持常见指标：MA、EMA、BOLL、ATR、RSI、MACD、SAR、Volume。

多线程批处理：独立计算线程池，按时间窗口增量更新；

扩展机制：用户可定义新指标（通过 C++ 插件或脚本接口）。

4. 策略引擎（后续阶段）

基于事件驱动（onTick/onBar）；

多策略并行运行、隔离线程；

本地仿真撮合（限价/市价/滑点/手续费）；

实盘 API 对接 Binance 下单/撤单接口。

5. 性能与监控

实时帧率、延迟、内存监控面板；

性能指标：

FPS ≥ 120（常态）；

tick→渲染延迟 P99 < 30ms；

CPU < 60%，内存 < 800MB。

6. 系统与安全

API Key 加密存储；

配置热更新；

崩溃日志与自动恢复；

单机运行，无外部依赖。

四、非功能需求
类别	说明
性能	实时渲染与指标计算在独立线程；支持 4 幅图并行且不卡顿
稳定性	网络断开后自动重连、状态恢复
扩展性	模块化架构：UI、行情、指标、策略分层
安全性	本地加密存储 API Key、只读模式
可维护性	模块与接口文档清晰、日志标准化
跨平台	优先 Windows 10/11；未来可迁移至 Avalonia 或 Vulkan
五、项目阶段与里程碑
阶段	目标	主要输出	预计工期
阶段 1：MVP 原型	实现 2×2 界面、实时行情、指标绘制、数据回放	可运行原型（无交易）	3–4 周
阶段 2：性能优化与指标扩展	批量指标、多线程优化、延迟监控	稳定 120FPS 原型	2–3 周
阶段 3：策略与仿真	策略引擎、回测、下单接口	可回测/模拟交易版本	3–4 周
阶段 4：实盘交易与部署	Binance 实盘下单、风控、安全	Beta 实盘版	3 周
阶段 5：完善与文档化	配置、日志、打包、说明书	正式发布版	2 周
六、团队分工建议
角色	职责
C++ 核心开发	ImGui/ImPlot 集成、DX11 渲染、性能优化
网络工程师	Asio WebSocket、REST API 封装、心跳重连
量化算法工程师	指标计算核优化、策略接口设计
测试工程师	延迟测试、压力测试、回放验证
UI/交互	Dock 布局、主题、快捷键交互
项目经理/架构师	模块拆分、接口规范、发布管理
七、技术关键点与难点

多线程架构设计：网络、计算、渲染三线程异步协作。

指标批量计算优化：滑动窗口增量算法，避免重复遍历。

渲染性能控制：ImPlot 数据点裁剪、GPU 批量绘制。

低延迟通信：Asio 异步读写 + 无锁队列传递。

数据同步：多周期 K 线对齐与时间线统一。

风控与安全：API 速率限制、异常撤单与状态同步。

八、交付与测试标准

可执行文件 + CMake 工程 + 源码文档；

延迟/性能测试报告；

指标结果对比验证（与 TradingView 或 Python TA-Lib）；

崩溃/异常测试报告；

代码覆盖率与静态分析报告。

九、未来扩展规划

GPU 加速指标计算（CUDA/OpenCL）；

多账户管理与策略沙盒；

跨平台（Linux/macOS）版本；

WebSocket 分布式行情聚合；

与 Python 策略接口（Pybind11）。

十、参考开源仓库
功能	开源库	链接
GUI 框架	Dear ImGui (docking)	https://github.com/ocornut/imgui

图表扩展	ImPlot	https://github.com/epezent/implot

指标计算	TA-Lib / Tulip Indicators	https://github.com/mrjbq7/ta-lib

异步通信	Asio / Boost.Asio	https://think-async.com/

并发队列	moodycamel::ConcurrentQueue	https://github.com/cameron314/concurrentqueue