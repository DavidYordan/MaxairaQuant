# QuantStudio 工作流记录

## 当前状态
- 完成 CMake + vcpkg 的骨架构建，主程序可运行。
- 集成网络栈基础（Boost.Asio/Beast + OpenSSL），WS 客户端最小连通版。
- 集成 UI 渲染环路：DX11 + ImGui + ImPlot 最小 Demo；已启用 Docking 并创建默认 DockSpace。
- 骨架、网络栈（Boost.Asio/Beast + OpenSSL）与 UI 环路（DX11 + ImGui + ImPlot）已就绪，启用了 Docking。
- 已接入 Binance `markPrice` 实时数据，并在 UI“Market”面板绘制实时价格曲线。

## 最近改动
- vcpkg 依赖修正：启用 `imgui` 的 `win32-binding`、`d3d11-binding`、`docking-experimental` 特性。
- 代码包含路径改为 `imgui/backends/...`，保证后端头文件可见。
- 在 UI 初始化中开启 `ImGuiConfigFlags_DockingEnable`，并使用 `ImGui::DockSpaceOverViewport()` 提供 2×2 等多视图布局能力。
- WS 客户端增加消息回调，App 中解析 JSON（单流/合并流），将价格写入线程安全的 `DataStore`。
- UI 渲染循环读取 `DataStore::snapshot()` 并用 ImPlot 绘制实时曲线；保留示例正弦曲线面板。
- 数据存储设置上限（5000 点），避免内存无界增长。

## 验证
- 重新配置与编译后运行 `QuantStudio.exe`，主窗口可进行 Docking（拖拽拆分面板），并显示 ImPlot 正弦曲线。
  - 可见两个面板：Market（实时价格）与 UI Demo（示例正弦波）。
  - Docking 生效，可拖拽拆分为 2×2 等布局。
  - 关闭窗口后程序正常退出。

## 下一步目标
- 事件管线优化：将当前互斥队列替换为 SPSC ring，实现低延迟快照发布。
- 绘图增强：按时间轴绘制（X=事件时间），增加指标叠加（MA/EMA）。
- 面板与布局：日志与性能监控面板，布局保存/加载（Ini/JSON）。
- 安全与健壮：开启 TLS 校验（编译选项 `QS_SSL_VERIFY=ON`），断线重连与速率控制。

## 实施计划
- 依赖与构建：
  - 在 `vcpkg.json` 增加 `boost-asio`、`boost-beast`、`boost-system`、`openssl`。
  - 在 `CMakeLists.txt` 中 `find_package(...)` 并链接。
- 网络模块：
  - 完成 `WSClient` 的 `start(url)` 和 `stop()`，支持 `wss://` 连接与读取循环。
  - 在 `App::start()` 启动一个默认流（示例：`wss://fstream.binance.com/ws/btcusdt@markPrice`），将消息输出到日志。
- 验证：
  - 重新配置与编译。
  - 运行后观察日志是否能打印 WebSocket 消息。

## 验证命令（Windows）
- 重新配置：
```
cmake -S . -B build -DCMAKE_TOOLCHAIN_FILE="d:\complex\MaxairaQuant\QuantStudio\tools\vcpkg\scripts\buildsystems\vcpkg.cmake" -DVCPKG_TARGET_TRIPLET=x64-windows -DCMAKE_BUILD_TYPE=Release
```
- 编译：
```
cmake --build build --config Release
```

## 风险与回退
- OpenSSL 证书校验暂以最简模式处理，后续需启用验证并加载系统/自定义 CA。
- 如果 vcpkg 包解析有差异，可改用 `Boost` 统一 `find_package(Boost COMPONENTS ...)`，或直接头文件引用（Beast/Asio），只链接 OpenSSL。