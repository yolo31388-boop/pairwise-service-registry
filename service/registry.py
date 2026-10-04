"""
游戏内服务发现与注册中心系统 - 核心模块

功能：
- 服务注册：携带元数据（版本/权重/区域）并初始化心跳
- 服务发现：带 TTL 缓存 + 加权轮询负载均衡
- 心跳：超时标记 + 重试容忍
- 健康检查：固定间隔 + 连续失败容错
- 服务注销：事件通知 + 缓存/连接清理
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


@dataclass
class Config:
    heartbeat_interval: float = 5.0
    heartbeat_timeout: float = 15.0
    heartbeat_max_retries: int = 3
    cache_ttl: float = 5.0
    health_check_interval: float = 10.0
    health_check_fail_threshold: int = 3


class ServiceRegistry:
    DEFAULT_CONFIG = {
        "heartbeat_interval": 5.0,
        "heartbeat_timeout": 15.0,
        "heartbeat_max_retries": 3,
        "cache_ttl": 5.0,
        "health_check_interval": 10.0,
        "health_check_fail_threshold": 3,
    }

    def __init__(self, config: Optional[Dict] = None):
        self.config = dict(self.DEFAULT_CONFIG)
        if config:
            self.config.update(config)
        self._services: Dict[str, Dict] = {}
        self._name_index: Dict[str, List[str]] = {}
        self._cache: Dict[str, Dict] = {}
        self._rr_index: Dict[str, int] = {}
        self._listeners: List[Callable] = []
        self._connections: Dict[str, Set] = {}
        self._last_health_check: Optional[float] = None
        self._now = 0.0
        self._id_counter = 0
        self._history: List = []

    # ---------------- 服务注册 ----------------

    def register_service(self, service_name: str, host: str, port: int,
                         metadata: Optional[Dict] = None) -> str:
        """注册服务实例，返回 service_id。元数据缺省补全 version/weight/region。"""
        meta = dict(metadata or {})
        meta.setdefault("version", "1.0.0")
        meta.setdefault("weight", 1)
        meta.setdefault("region", "default")

        self._id_counter += 1
        service_id = "%s-%d" % (service_name, self._id_counter)
        info = {
            "id": service_id,
            "name": service_name,
            "host": host,
            "port": port,
            "metadata": meta,
            "status": "healthy",
            "registered_at": self._now,
            "last_heartbeat": self._now,
            "missed_heartbeats": 0,
            "consecutive_check_failures": 0,
        }
        self._services[service_id] = info
        self._name_index.setdefault(service_name, []).append(service_id)
        self._invalidate_cache(service_name)
        self._notify("register", info)
        return service_id

    # ---------------- 服务发现 ----------------

    def discover_service(self, service_name: str) -> Optional[Dict]:
        """按服务名发现一个健康实例：优先读缓存，多实例按权重轮询。"""
        instances = self._cached_instances(service_name)
        if instances is None:
            instances = [
                self._services[sid]
                for sid in self._name_index.get(service_name, [])
                if sid in self._services
                and self._services[sid]["status"] == "healthy"
            ]
            self._cache[service_name] = {
                "expires": self._now + self.config["cache_ttl"],
                "instances": instances,
            }
        if not instances:
            return None

        pool = []
        for inst in instances:
            weight = max(1, int(inst["metadata"].get("weight", 1)))
            pool.extend([inst] * weight)
        idx = self._rr_index.get(service_name, 0)
        self._rr_index[service_name] = idx + 1
        return pool[idx % len(pool)]

    def _cached_instances(self, service_name: str) -> Optional[List[Dict]]:
        entry = self._cache.get(service_name)
        if entry is not None and entry["expires"] > self._now:
            return entry["instances"]
        return None

    def _invalidate_cache(self, service_name: str):
        self._cache.pop(service_name, None)

    # ---------------- 心跳 ----------------

    def heartbeat(self, service_id: str) -> bool:
        """刷新心跳；未知服务返回 False。心跳可让异常实例恢复健康。"""
        info = self._services.get(service_id)
        if info is None:
            return False
        info["last_heartbeat"] = self._now
        info["missed_heartbeats"] = 0
        if info["status"] != "healthy":
            info["status"] = "healthy"
            self._invalidate_cache(info["name"])
        return True

    def update(self, dt: float):
        """推进逻辑时钟：统计丢失心跳，超过重试次数标记为不健康。"""
        self._now += dt
        interval = self.config["heartbeat_interval"]
        max_retries = self.config["heartbeat_max_retries"]
        for service_id, info in list(self._services.items()):
            elapsed = self._now - info["last_heartbeat"]
            info["missed_heartbeats"] = int(elapsed // interval)
            if (info["missed_heartbeats"] > max_retries
                    and info["status"] == "healthy"):
                info["status"] = "unhealthy"
                self._invalidate_cache(info["name"])
                self._notify("unhealthy", info)

    # ---------------- 健康检查 ----------------

    def health_check(self) -> Dict:
        """按间隔巡检；连续失败达到阈值才摘除实例（容错）。"""
        interval = self.config["health_check_interval"]
        if (self._last_health_check is not None
                and self._now - self._last_health_check < interval):
            return {"checked": 0, "removed": [], "skipped": True}
        self._last_health_check = self._now

        threshold = self.config["health_check_fail_threshold"]
        checked = 0
        removed = []
        for service_id, info in list(self._services.items()):
            checked += 1
            if self._check_instance(info):
                info["consecutive_check_failures"] = 0
            else:
                info["consecutive_check_failures"] += 1
                if info["consecutive_check_failures"] >= threshold:
                    self.service_deregister(service_id)
                    removed.append(service_id)
        return {"checked": checked, "removed": removed, "skipped": False}

    def _check_instance(self, info: Dict) -> bool:
        checker = self.config.get("health_checker")
        if callable(checker):
            return bool(checker(info))
        return (self._now - info["last_heartbeat"]
                <= self.config["heartbeat_timeout"])

    # ---------------- 服务注销 ----------------

    def service_deregister(self, service_id: str) -> bool:
        """注销服务：移除注册信息、失效缓存、释放连接并通知订阅者。"""
        info = self._services.pop(service_id, None)
        if info is None:
            return False
        ids = self._name_index.get(info["name"])
        if ids and service_id in ids:
            ids.remove(service_id)
        self._invalidate_cache(info["name"])

        for conn in self._connections.pop(service_id, set()):
            close = getattr(conn, "close", None)
            if callable(close):
                close()

        self._notify("deregister", info)
        return True

    # ---------------- 通知与连接 ----------------

    def subscribe(self, listener: Callable):
        if listener not in self._listeners:
            self._listeners.append(listener)

    def unsubscribe(self, listener: Callable):
        if listener in self._listeners:
            self._listeners.remove(listener)

    def _notify(self, event: str, info: Dict):
        record = {"event": event, "service_id": info["id"], "time": self._now}
        self._history.append(record)
        for listener in list(self._listeners):
            listener(event, dict(info))

    def add_connection(self, service_id: str, conn):
        self._connections.setdefault(service_id, set()).add(conn)

    # ---------------- 其他 ----------------

    def list_services(self, service_name: Optional[str] = None) -> List[Dict]:
        if service_name is None:
            return [dict(info) for info in self._services.values()]
        return [dict(self._services[sid])
                for sid in self._name_index.get(service_name, [])
                if sid in self._services]

    def reset(self):
        self._services = {}
        self._name_index = {}
        self._cache = {}
        self._rr_index = {}
        self._listeners = []
        self._connections = {}
        self._last_health_check = None
        self._now = 0.0
        self._id_counter = 0
        self._history = []
