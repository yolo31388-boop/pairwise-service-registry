"""
游戏内服务发现与注册中心系统 - 核心模块

功能：
- 服务注册：真实写入注册中心，携带元数据（版本/权重/区域），并启动心跳跟踪
- 服务发现：本地缓存 + 加权轮询负载均衡，无需硬编码 IP
- 心跳：失败自动重试，超时标记不健康，长期不心跳自动注销
- 健康检查：按最小间隔执行，连续失败达到阈值才摘除（容错）
- 服务注销：通知订阅方并清理发现缓存与相关连接
"""
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, List, Optional


DEFAULT_CONFIG: Dict[str, float] = {
    "heartbeat_interval": 5.0,        # 心跳建议间隔（秒）
    "heartbeat_timeout": 15.0,        # 超过该时间未收到心跳 -> 标记不健康
    "offline_timeout": 30.0,          # 超过该时间未收到心跳 -> 自动注销
    "heartbeat_max_retries": 3,       # 单次心跳发送失败后的最大重试次数
    "health_check_interval": 10.0,    # 健康检查最小间隔（秒）
    "health_check_max_failures": 3,   # 连续失败达到该次数才摘除服务
    "cache_ttl": 5.0,                 # 服务发现缓存有效期（秒）
}

STATUS_HEALTHY = "healthy"
STATUS_UNHEALTHY = "unhealthy"


class HeartbeatError(Exception):
    """心跳发送失败（网络抖动等原因）。"""


@dataclass
class Config:
    heartbeat_interval: float = 5.0
    heartbeat_timeout: float = 15.0
    offline_timeout: float = 30.0
    heartbeat_max_retries: int = 3
    health_check_interval: float = 10.0
    health_check_max_failures: int = 3
    cache_ttl: float = 5.0


@dataclass
class ServiceInstance:
    instance_id: str
    service_name: str
    host: str
    port: int
    metadata: Dict = field(default_factory=dict)
    status: str = STATUS_HEALTHY
    registered_at: float = 0.0
    last_heartbeat: float = 0.0
    missed_heartbeats: int = 0
    consecutive_check_failures: int = 0

    @property
    def weight(self) -> int:
        try:
            return max(1, int(self.metadata.get("weight", 1)))
        except (TypeError, ValueError):
            return 1

    def to_dict(self) -> Dict:
        return {
            "instance_id": self.instance_id,
            "service_name": self.service_name,
            "host": self.host,
            "port": self.port,
            "metadata": dict(self.metadata),
            "status": self.status,
        }


class ServiceRegistry:
    def __init__(self, config: Optional[Dict] = None):
        self.config = dict(DEFAULT_CONFIG)
        if config is not None:
            self.config.update(asdict(config) if isinstance(config, Config) else config)
        self._instances: Dict[str, ServiceInstance] = {}
        self._services: Dict[str, List[str]] = {}
        self._cache: Dict[str, tuple] = {}
        self._rr_counters: Dict[str, int] = {}
        self._subscribers: Dict[str, List[Callable]] = {}
        self._connections: Dict[str, List] = {}
        self._notifications: List[Dict] = []
        self._history: List[Dict] = []
        self._clock = 0.0
        self._last_health_check: Optional[float] = None
        self._id_counter = 0
        self.cache_stats = {"hits": 0, "misses": 0}
        self.health_check_count = 0

    # ------------------------------------------------------------------ 基础

    def _now(self) -> float:
        return self._clock

    def reset(self):
        self._instances.clear()
        self._services.clear()
        self._cache.clear()
        self._rr_counters.clear()
        self._subscribers.clear()
        for connections in self._connections.values():
            for conn in connections:
                self._close_connection(conn)
        self._connections.clear()
        self._notifications.clear()
        self._history.clear()
        self._clock = 0.0
        self._last_health_check = None
        self._id_counter = 0
        self.cache_stats = {"hits": 0, "misses": 0}
        self.health_check_count = 0

    def update(self, dt: float):
        """推进逻辑时钟，处理心跳超时与到期的健康检查。"""
        self._clock += dt
        self._scan_heartbeat_timeouts()
        if (
            self._last_health_check is None
            or self._clock - self._last_health_check
            >= self.config["health_check_interval"]
        ):
            self.health_check()

    def get_instance(self, instance_id: str) -> Optional[ServiceInstance]:
        return self._instances.get(instance_id)

    def list_instances(self, service_name: str) -> List[ServiceInstance]:
        return [
            self._instances[iid]
            for iid in self._services.get(service_name, [])
            if iid in self._instances
        ]

    # ------------------------------------------------------------ 服务注册

    def register_service(
        self,
        service_name: str,
        host: str = "127.0.0.1",
        port: int = 0,
        metadata: Optional[Dict] = None,
        version: str = "1.0.0",
        weight: int = 1,
        region: str = "default",
    ) -> str:
        """把服务实例真实写入注册中心，并立即开始心跳跟踪。返回实例 ID。"""
        self._id_counter += 1
        instance_id = f"{service_name}-{self._id_counter}"
        meta = {"version": version, "weight": weight, "region": region}
        if metadata:
            meta.update(metadata)
        now = self._now()
        instance = ServiceInstance(
            instance_id=instance_id,
            service_name=service_name,
            host=host,
            port=port,
            metadata=meta,
            status=STATUS_HEALTHY,
            registered_at=now,
            last_heartbeat=now,
        )
        self._instances[instance_id] = instance
        self._services.setdefault(service_name, []).append(instance_id)
        self._invalidate_cache(service_name)
        self._record("register", instance, "ok")
        return instance_id

    # ------------------------------------------------------------ 服务发现

    def discover_service(self, service_name: str) -> Optional[Dict]:
        """按负载均衡策略返回一个健康实例；没有可用实例时返回 None。"""
        instance_ids = self._get_healthy_instance_ids(service_name)
        if not instance_ids:
            return None
        chosen = self._load_balance(service_name, instance_ids)
        return self._instances[chosen].to_dict()

    def _get_healthy_instance_ids(self, service_name: str) -> List[str]:
        now = self._now()
        cached = self._cache.get(service_name)
        if cached is not None and now - cached[0] < self.config["cache_ttl"]:
            self.cache_stats["hits"] += 1
            return list(cached[1])
        self.cache_stats["misses"] += 1
        healthy_ids = [
            iid
            for iid in self._services.get(service_name, [])
            if iid in self._instances
            and self._instances[iid].status == STATUS_HEALTHY
        ]
        self._cache[service_name] = (now, list(healthy_ids))
        return healthy_ids

    def _load_balance(self, service_name: str, instance_ids: List[str]) -> str:
        """加权轮询：权重越大的实例被选中的频率越高。"""
        weighted_pool: List[str] = []
        for instance_id in instance_ids:
            weighted_pool.extend(
                [instance_id] * self._instances[instance_id].weight
            )
        index = self._rr_counters.get(service_name, 0)
        self._rr_counters[service_name] = index + 1
        return weighted_pool[index % len(weighted_pool)]

    def _invalidate_cache(self, service_name: str):
        self._cache.pop(service_name, None)

    # ---------------------------------------------------------------- 心跳

    def heartbeat(self, instance_id: str) -> bool:
        """上报心跳；发送失败时自动重试，重试耗尽计为一次心跳丢失。"""
        instance = self._instances.get(instance_id)
        if instance is None:
            return False
        max_attempts = int(self.config["heartbeat_max_retries"]) + 1
        for _ in range(max_attempts):
            try:
                self._send_heartbeat(instance)
            except HeartbeatError:
                continue
            instance.last_heartbeat = self._now()
            instance.missed_heartbeats = 0
            if instance.status != STATUS_HEALTHY:
                instance.status = STATUS_HEALTHY
                self._invalidate_cache(instance.service_name)
            return True
        instance.missed_heartbeats += 1
        return False

    def _send_heartbeat(self, instance: ServiceInstance):
        """实际发送心跳，默认可靠；网络抖动时抛出 HeartbeatError 触发重试。"""
        return True

    def _scan_heartbeat_timeouts(self):
        now = self._now()
        for instance in list(self._instances.values()):
            stale = now - instance.last_heartbeat
            if stale > self.config["offline_timeout"]:
                self.service_deregister(
                    instance.instance_id, reason="heartbeat_timeout"
                )
            elif (
                stale > self.config["heartbeat_timeout"]
                and instance.status == STATUS_HEALTHY
            ):
                instance.status = STATUS_UNHEALTHY
                self._invalidate_cache(instance.service_name)
                self._record("unhealthy", instance, "heartbeat_timeout")

    # ------------------------------------------------------------- 健康检查

    def health_check(self, force: bool = False) -> Dict:
        """对所有实例做一次健康检查，受最小间隔限制（force 可跳过限制）。"""
        now = self._now()
        if (
            not force
            and self._last_health_check is not None
            and now - self._last_health_check < self.config["health_check_interval"]
        ):
            return {
                "skipped": True,
                "checked": 0,
                "healthy": [],
                "failed": [],
                "deregistered": [],
                "timestamp": now,
            }
        self._last_health_check = now
        self.health_check_count += 1
        healthy: List[str] = []
        failed: List[str] = []
        deregistered: List[str] = []
        for instance in list(self._instances.values()):
            if self._probe(instance):
                instance.consecutive_check_failures = 0
                healthy.append(instance.instance_id)
            else:
                instance.consecutive_check_failures += 1
                failed.append(instance.instance_id)
                if (
                    instance.consecutive_check_failures
                    >= self.config["health_check_max_failures"]
                ):
                    self.service_deregister(
                        instance.instance_id, reason="health_check_failed"
                    )
                    deregistered.append(instance.instance_id)
        return {
            "skipped": False,
            "checked": len(healthy) + len(failed),
            "healthy": healthy,
            "failed": failed,
            "deregistered": deregistered,
            "timestamp": now,
        }

    def _probe(self, instance: ServiceInstance) -> bool:
        """健康探测：心跳未超时即视为健康。"""
        return self._now() - instance.last_heartbeat <= self.config["heartbeat_timeout"]

    # ------------------------------------------------------------- 服务注销

    def service_deregister(self, instance_id: str, reason: str = "manual") -> bool:
        """摘除实例：移出注册表、清理缓存与连接，并通知订阅方。"""
        instance = self._instances.pop(instance_id, None)
        if instance is None:
            return False
        ids = self._services.get(instance.service_name)
        if ids is not None and instance_id in ids:
            ids.remove(instance_id)
            if not ids:
                self._services.pop(instance.service_name, None)
        self._invalidate_cache(instance.service_name)
        self._rr_counters.pop(instance.service_name, None)
        for connection in self._connections.pop(instance_id, []):
            self._close_connection(connection)
        event = {
            "type": "deregister",
            "service_name": instance.service_name,
            "instance_id": instance_id,
            "host": instance.host,
            "port": instance.port,
            "reason": reason,
            "timestamp": self._now(),
        }
        self._notifications.append(event)
        self._history.append(event)
        callbacks = list(self._subscribers.get(instance.service_name, []))
        callbacks.extend(self._subscribers.get("*", []))
        for callback in callbacks:
            callback(event)
        return True

    def subscribe(self, service_name: str, callback: Callable[[Dict], None]):
        """订阅某服务（或用 "*" 订阅全部）的下线通知。"""
        self._subscribers.setdefault(service_name, []).append(callback)

    def get_notifications(self, service_name: Optional[str] = None) -> List[Dict]:
        if service_name is None:
            return list(self._notifications)
        return [
            event
            for event in self._notifications
            if event["service_name"] == service_name
        ]

    # --------------------------------------------------------- 连接资源管理

    def register_connection(self, instance_id: str, connection):
        """登记与某实例相关的连接，实例注销时统一释放。"""
        if instance_id not in self._instances:
            return False
        self._connections.setdefault(instance_id, []).append(connection)
        return True

    @staticmethod
    def _close_connection(connection):
        close = getattr(connection, "close", None)
        if callable(close):
            close()

    # --------------------------------------------------------------- 内部

    def _record(self, event_type: str, instance: ServiceInstance, reason: str):
        self._history.append(
            {
                "type": event_type,
                "service_name": instance.service_name,
                "instance_id": instance.instance_id,
                "reason": reason,
                "timestamp": self._now(),
            }
        )
