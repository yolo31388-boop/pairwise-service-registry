"""
游戏内服务发现与注册中心系统 - 验收测试
运行方式：python -m pytest tests/test_registry.py -q
共 15 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.registry import ServiceRegistry


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class TestServiceRegistry:
    def setup_method(self):
        self.system = ServiceRegistry()

    # ---------- 服务注册 ----------

    def test_case_01(self):
        """注册后实例真实写入注册中心，可通过发现获取"""
        sid = self.system.register_service("gateway", "10.0.0.1", 8001)
        assert sid in self.system._services
        found = self.system.discover_service("gateway")
        assert found is not None
        assert found["host"] == "10.0.0.1"
        assert found["port"] == 8001

    def test_case_02(self):
        """注册携带元数据：版本、权重、区域"""
        sid = self.system.register_service(
            "battle", "10.0.0.2", 8002,
            metadata={"version": "2.1.0", "weight": 5, "region": "cn-east"})
        meta = self.system._services[sid]["metadata"]
        assert meta["version"] == "2.1.0"
        assert meta["weight"] == 5
        assert meta["region"] == "cn-east"

    def test_case_03(self):
        """注册时初始化心跳时间戳与健康状态"""
        sid = self.system.register_service("chat", "10.0.0.3", 8003)
        info = self.system._services[sid]
        assert info["status"] == "healthy"
        assert info["last_heartbeat"] == info["registered_at"]
        assert info["missed_heartbeats"] == 0

    # ---------- 服务发现 ----------

    def test_case_04(self):
        """按服务名发现，无需硬编码 IP，且能区分不同服务"""
        self.system.register_service("gateway", "10.0.0.1", 8001)
        self.system.register_service("battle", "10.0.0.2", 8002)
        assert self.system.discover_service("gateway")["port"] == 8001
        assert self.system.discover_service("battle")["port"] == 8002
        assert self.system.discover_service("nonexistent") is None

    def test_case_05(self):
        """发现结果带缓存：TTL 内不回源，过期后重新查询"""
        sid = self.system.register_service("gateway", "10.0.0.1", 8001)
        first = self.system.discover_service("gateway")
        assert first is not None
        # 绕过注销接口直接删库，模拟注册中心数据变化
        del self.system._services[sid]
        # 缓存未过期，仍返回缓存结果
        assert self.system.discover_service("gateway") is not None
        # 缓存过期后重新查询，拿到最新（空）结果
        self.system.update(self.system.config["cache_ttl"] + 1)
        assert self.system.discover_service("gateway") is None

    def test_case_06(self):
        """负载均衡：多实例轮询，不总是返回第一个"""
        ids = [self.system.register_service("battle", "10.0.0.%d" % i, 8000 + i)
               for i in range(1, 4)]
        picked = {self.system.discover_service("battle")["id"] for _ in range(3)}
        assert picked == set(ids)

    def test_case_07(self):
        """负载均衡按元数据权重分配流量"""
        heavy = self.system.register_service(
            "battle", "10.0.0.1", 8001, metadata={"weight": 2})
        light = self.system.register_service(
            "battle", "10.0.0.2", 8002, metadata={"weight": 1})
        picked = [self.system.discover_service("battle")["id"] for _ in range(3)]
        assert picked.count(heavy) == 2
        assert picked.count(light) == 1

    # ---------- 心跳 ----------

    def test_case_08(self):
        """心跳刷新时间戳并清零丢失计数"""
        sid = self.system.register_service("chat", "10.0.0.3", 8003)
        self.system.update(7)
        assert self.system.heartbeat(sid) is True
        info = self.system._services[sid]
        assert info["last_heartbeat"] == self.system._now
        assert info["missed_heartbeats"] == 0
        assert self.system.heartbeat("ghost-id") is False

    def test_case_09(self):
        """心跳超时且超过重试次数：实例被标记不健康并退出发现列表"""
        sid = self.system.register_service("chat", "10.0.0.3", 8003)
        interval = self.system.config["heartbeat_interval"]
        retries = self.system.config["heartbeat_max_retries"]
        self.system.update(interval * (retries + 1) + 1)
        assert self.system._services[sid]["status"] == "unhealthy"
        assert self.system.discover_service("chat") is None

    def test_case_10(self):
        """心跳重试容忍：偶发丢失（网络抖动）不会被误判"""
        sid = self.system.register_service("chat", "10.0.0.3", 8003)
        interval = self.system.config["heartbeat_interval"]
        retries = self.system.config["heartbeat_max_retries"]
        self.system.update(interval * retries)  # 丢失次数未超阈值
        assert self.system._services[sid]["status"] == "healthy"
        assert self.system.discover_service("chat") is not None

    # ---------- 健康检查 ----------

    def test_case_11(self):
        """健康检查摘除连续失败的死实例"""
        sid = self.system.register_service("mail", "10.0.0.4", 8004)
        interval = self.system.config["health_check_interval"]
        threshold = self.system.config["health_check_fail_threshold"]
        # 让心跳彻底过期，然后逐轮检查直至达到失败阈值
        self.system.update(self.system.config["heartbeat_timeout"] + 1)
        for _ in range(threshold):
            result = self.system.health_check()
            self.system.update(interval)
        assert sid in result["removed"] or sid not in self.system._services
        assert sid not in self.system._services

    def test_case_12(self):
        """健康检查有间隔：间隔内重复调用直接跳过"""
        self.system.register_service("mail", "10.0.0.4", 8004)
        first = self.system.health_check()
        assert first["checked"] == 1 and not first["skipped"]
        second = self.system.health_check()
        assert second["skipped"] and second["checked"] == 0

    def test_case_13(self):
        """健康检查容错：偶发失败不摘除，恢复后失败计数清零"""
        sid = self.system.register_service("mail", "10.0.0.4", 8004)
        interval = self.system.config["health_check_interval"]
        threshold = self.system.config["health_check_fail_threshold"]
        # 连续失败 threshold-1 次
        self.system.update(self.system.config["heartbeat_timeout"] + 1)
        for _ in range(threshold - 1):
            self.system.health_check()
            self.system.update(interval)
        # 心跳恢复，下一轮检查通过并清零失败计数
        self.system.heartbeat(sid)
        self.system.health_check()
        assert self.system._services[sid]["consecutive_check_failures"] == 0
        # 再次失败 threshold-1 次，仍未达到连续阈值，不摘除
        self.system.update(self.system.config["heartbeat_timeout"] + 1)
        for _ in range(threshold - 1):
            self.system.health_check()
            self.system.update(interval)
        assert sid in self.system._services

    # ---------- 服务注销 ----------

    def test_case_14(self):
        """注销后实例从注册列表和发现缓存中移除"""
        sid = self.system.register_service("gateway", "10.0.0.1", 8001)
        assert self.system.discover_service("gateway") is not None  # 建立缓存
        assert self.system.service_deregister(sid) is True
        assert sid not in self.system._services
        assert self.system.discover_service("gateway") is None
        assert self.system.service_deregister(sid) is False

    def test_case_15(self):
        """注销时通知订阅者并清理相关连接"""
        events = []
        self.system.subscribe(lambda event, info: events.append((event, info["id"])))
        sid = self.system.register_service("gateway", "10.0.0.1", 8001)
        conn = FakeConnection()
        self.system.add_connection(sid, conn)
        self.system.service_deregister(sid)
        assert ("deregister", sid) in events
        assert conn.closed is True
        assert sid not in self.system._connections


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
