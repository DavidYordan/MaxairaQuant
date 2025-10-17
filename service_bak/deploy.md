# MaxairaQuant 数据收集器部署配置

## 服务器规格
- CPU: 16核
- 内存: 32GB
- 时区: UTC

## 宝塔面板启动命令（精简配置）

```bash
/www/server/java/jdk-21.0.2/bin/java -jar \
   -Xms8G \
   -Xmx24G \
   -XX:+UseG1GC \
   -XX:MaxGCPauseMillis=100 \
   -Djava.awt.headless=true \
   -Duser.timezone=UTC \
   /www/wwwroot/MaxairaQuant/maxaira-datacollector-1.0.0.jar
```

## 必要参数说明

### 内存配置（必须）
- `-Xms8G`: 初始堆内存8GB，避免启动时内存扩容影响性能
- `-Xmx24G`: 最大堆内存24GB，为32GB服务器预留8GB给系统

### 垃圾收集器（推荐）
- `-XX:+UseG1GC`: 使用G1垃圾收集器，适合大内存低延迟场景
- `-XX:MaxGCPauseMillis=100`: GC暂停时间目标100ms，适合实时数据处理

### 系统配置（必须）
- `-Djava.awt.headless=true`: 无头模式，服务器环境必需
- `-Duser.timezone=UTC`: **时区设置为UTC，确保与币安API时间一致**

## 监控建议

建议在宝塔面板中配置：
- JVM内存使用率告警（>80%）
- GC频率监控
- 应用响应时间监控