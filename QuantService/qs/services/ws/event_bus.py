from __future__ import annotations
import asyncio
from loguru import logger
from typing import Dict, List, Any
from collections import defaultdict
import weakref


class EventBus:
    """改进的异步事件总线 - 支持优先级和背压控制"""
    
    def __init__(self):
        self._subs: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._stats = {
            'published': 0,
            'delivered': 0,
            'dropped': 0,
            'queue_full_events': 0
        }
        self._weak_refs: Dict[str, List] = defaultdict(list)  # 弱引用追踪

    async def subscribe(self, topic: str, maxsize: int = 100, priority: bool = False) -> asyncio.Queue:
        """订阅主题
        
        Args:
            topic: 主题名称
            maxsize: 队列最大大小
            priority: 是否为优先级订阅者（不会被丢弃消息）
        """
        queue = asyncio.Queue(maxsize=maxsize)
        queue._priority = priority  # 标记优先级
        queue._topic = topic  # 记录主题
        
        self._subs[topic].append(queue)
        
        # 使用弱引用追踪队列，自动清理
        weak_queue = weakref.ref(queue, lambda ref: self._cleanup_queue(topic, ref))
        self._weak_refs[topic].append(weak_queue)
        
        logger.debug(f"新订阅者加入主题 '{topic}', 队列大小: {maxsize}, 优先级: {priority}")
        return queue

    def _cleanup_queue(self, topic: str, weak_ref):
        """清理已销毁的队列引用"""
        try:
            self._weak_refs[topic].remove(weak_ref)
            if not self._weak_refs[topic]:
                del self._weak_refs[topic]
        except (ValueError, KeyError):
            pass

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        """取消订阅"""
        queues = self._subs.get(topic, [])
        try:
            queues.remove(queue)
            logger.debug(f"取消订阅主题 '{topic}'")
        except ValueError:
            pass
        
        if not queues:
            self._subs.pop(topic, None)

    async def publish(self, topic: str, payload: Any, timeout: float = 0.1) -> int:
        """发布消息到主题
        
        Args:
            topic: 主题名称
            payload: 消息载荷
            timeout: 发布超时时间
            
        Returns:
            成功投递的订阅者数量
        """
        queues = self._subs.get(topic, [])
        if not queues:
            return 0
        
        self._stats['published'] += 1
        delivered_count = 0
        
        # 创建发布任务列表
        publish_tasks = []
        for queue in list(queues):  # 复制列表避免并发修改
            task = asyncio.create_task(
                self._deliver_to_queue(queue, payload, topic),
                name=f"deliver_{topic}_{id(queue)}"
            )
            publish_tasks.append(task)
        
        # 并发投递消息
        if publish_tasks:
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*publish_tasks, return_exceptions=True),
                    timeout=timeout
                )
                delivered_count = sum(1 for result in results if result is True)
            except asyncio.TimeoutError:
                logger.warning(f"主题 '{topic}' 消息投递超时")
                # 取消未完成的任务
                for task in publish_tasks:
                    if not task.done():
                        task.cancel()
        
        self._stats['delivered'] += delivered_count
        return delivered_count

    async def _deliver_to_queue(self, queue: asyncio.Queue, payload: Any, topic: str) -> bool:
        """投递消息到单个队列"""
        try:
            # 检查队列是否仍然有效
            if queue.qsize() >= queue.maxsize:
                return await self._handle_queue_full(queue, payload, topic)
            
            queue.put_nowait(payload)
            return True
            
        except asyncio.QueueFull:
            return await self._handle_queue_full(queue, payload, topic)
        except Exception as e:
            logger.error(f"投递消息到队列失败 (主题: {topic}): {e}")
            return False

    async def _handle_queue_full(self, queue: asyncio.Queue, payload: Any, topic: str) -> bool:
        """处理队列满的情况"""
        self._stats['queue_full_events'] += 1
        
        # 优先级队列不丢弃消息
        if getattr(queue, '_priority', False):
            try:
                # 等待队列有空间（短暂等待）
                await asyncio.wait_for(queue.put(payload), timeout=0.05)
                return True
            except asyncio.TimeoutError:
                logger.warning(f"优先级队列仍然满 (主题: {topic})")
                return False
        
        # 非优先级队列：丢弃最老消息，添加新消息
        try:
            _ = queue.get_nowait()  # 移除最老消息
            queue.put_nowait(payload)  # 添加新消息
            self._stats['dropped'] += 1
            
            if self._stats['dropped'] % 100 == 0:  # 每100次记录一次
                logger.warning(
                    f"EventBus 已丢弃 {self._stats['dropped']} 条消息 "
                    f"(主题: {topic}, 队列大小: {queue.qsize()})"
                )
            return True
            
        except asyncio.QueueEmpty:
            # 队列为空但仍然满？异常情况
            logger.error(f"队列状态异常 (主题: {topic})")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """获取事件总线统计信息"""
        return {
            **self._stats,
            'active_topics': len(self._subs),
            'total_subscribers': sum(len(queues) for queues in self._subs.values()),
            'topics': {
                topic: {
                    'subscribers': len(queues),
                    'queue_sizes': [q.qsize() for q in queues if not q._closed]
                }
                for topic, queues in self._subs.items()
            }
        }

    async def close(self):
        """关闭事件总线，清理所有资源"""
        logger.info("正在关闭 EventBus...")
        
        # 清空所有队列
        for topic, queues in self._subs.items():
            for queue in queues:
                try:
                    while not queue.empty():
                        queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
        
        self._subs.clear()
        self._weak_refs.clear()
        
        logger.info(f"EventBus 已关闭. 最终统计: {self._stats}")