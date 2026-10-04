"""
游戏内服务发现与注册中心系统 - 验收测试
运行方式：python -m pytest tests/test_registry.py -q
共 15 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from service.registry import HeartbeatError, ServiceRegistry


class TestServiceRegistry:
    def setup_method(self):
        self.system = ServiceRegistry()

    # ---------------------------------------------------------- 服务注册

    def test_case_01(self):
        """注册真实写入注册中心，注册后立刻可被发现（不再只设标志位）。"""
        instance_id = self.system.register_service("gateway", "10.0.0.1", 8001)
        assert self.system.get_instance(instance_id) is not None
        found = self.system.discover_service("gateway")
        assert found is not None
        assert found["host"] == "10.0.0.1"
        assert found["port"] == 8001

    def test_case_02(self):
        """注册即启动心跳跟踪，心跳时间戳被初始化。"""
        instance_id = self.system.register_service("gateway", "10.0.0.1", 8001)
        instance = self.system.get_instance(instance_id)
        assert instance.last_heartbeat == self.system._now()
        assert instance.missed_heartbeats == 0
        assert instance.status == "healthy"

    def test_case_03(self):
        """注册携带元数据：版本、权重、区域均可查询。"""
        self.system.register_service(
            "gateway",
            "10.0.0.1",
            8001,
            version="2.1.0",
            weight=5,
            region="cn-east",
            metadata={"env": "prod"},
        )
        found = self.system.discover_service("gateway")
        assert found["metadata"]["version"] == "2.1.0"
        assert found["metadata"]["weight"] == 5
        assert found["metadata"]["region"] == "cn-east"
        assert found["metadata"]["env"] == "prod"

    # ---------------------------------------------------------- 服务发现

    def test_case_04(self):
        """发现基于注册中心而非硬编码 IP；未注册的服务返回 None。"""
        assert self.system.discover_service("not-exist") is None
        self.system.register_service("lobby", "10.0.1.2", 9001)
        found = self.system.discover_service("lobby")
        assert found is not None
        assert found["host"] == "10.0.1.2"

    def test_case_05(self):
        """发现结果有缓存：TTL 内重复查询不再回源注册表。"""
        self.system.register_service("lobby", "10.0.1.2", 9001)
        self.system.discover_service("lobby")
        misses = self.system.cache_stats["misses"]
        self.system.discover_service("lobby")
        self.system.discover_service("lobby")
        assert self.system.cache_stats["hits"] == 2
        assert self.system.cache_stats["misses"] == misses

    def test_case_06(self):
        """负载均衡：多实例轮询分发，且权重高的实例分到更多流量。"""
        ids = [
            self.system.register_service("game", f"10.0.2.{i}", 7000 + i)
            for i in range(1, 4)
        ]
        picks = [self.system.discover_service("game")["instance_id"] for _ in range(6)]
        for instance_id in ids:
            assert picks.count(instance_id) == 2

        heavy = self.system.register_service("rank", "10.0.3.1", 6001, weight=2)
        light = self.system.register_service("rank", "10.0.3.2", 6002, weight=1)
        weighted = [
            self.system.discover_service("rank")["instance_id"] for _ in range(3)
        ]
        assert weighted.count(heavy) == 2
        assert weighted.count(light) == 1

    # -------------------------------------------------------------- 心跳

    def test_case_07(self):
        """心跳刷新心跳时间戳，保持实例在线。"""
        instance_id = self.system.register_service("game", "10.0.2.1", 7001)
        self.system.update(4.0)
        assert self.system.heartbeat(instance_id) is True
        instance = self.system.get_instance(instance_id)
        assert instance.last_heartbeat == self.system._now()
        assert instance.missed_heartbeats == 0

    def test_case_08(self):
        """心跳超时：超时未更新被标记不健康并摘除，长期失联自动注销。"""
        instance_id = self.system.register_service("game", "10.0.2.1", 7001)
        self.system.update(16.0)  # 超过 heartbeat_timeout=15
        instance = self.system.get_instance(instance_id)
        assert instance is not None
        assert instance.status == "unhealthy"
        assert self.system.discover_service("game") is None
        self.system.update(20.0)  # 累计 36s，超过 offline_timeout=30
        assert self.system.get_instance(instance_id) is None

    def test_case_09(self):
        """心跳重试：网络抖动时自动重试，重试耗尽才计为丢失。"""
        instance_id = self.system.register_service("game", "10.0.2.1", 7001)
        attempts = {"count": 0}

        def flaky(instance):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise HeartbeatError("network jitter")
            return True

        self.system._send_heartbeat = flaky
        assert self.system.heartbeat(instance_id) is True
        assert attempts["count"] == 3

        def always_fail(instance):
            raise HeartbeatError("network down")

        self.system._send_heartbeat = always_fail
        assert self.system.heartbeat(instance_id) is False
        assert self.system.get_instance(instance_id).missed_heartbeats == 1

    # ---------------------------------------------------------- 健康检查

    def test_case_10(self):
        """健康检查能摘除挂掉的服务，不再留在注册列表里。"""
        instance_id = self.system.register_service("chat", "10.0.4.1", 5001)
        self.system._probe = lambda instance: False
        for _ in range(self.system.config["health_check_max_failures"]):
            self.system.health_check(force=True)
        assert self.system.get_instance(instance_id) is None
        assert self.system.discover_service("chat") is None

    def test_case_11(self):
        """健康检查有最小间隔：间隔内的调用被跳过，避免过于频繁。"""
        self.system.register_service("chat", "10.0.4.1", 5001)
        self.system.health_check()
        assert self.system.health_check_count == 1
        result = self.system.health_check()
        assert result["skipped"] is True
        assert self.system.health_check_count == 1
        self.system.update(11.0)  # 超过 health_check_interval=10
        assert self.system.health_check_count == 2

    def test_case_12(self):
        """健康检查容错：单次失败不移除，恢复后失败计数清零。"""
        instance_id = self.system.register_service("chat", "10.0.4.1", 5001)
        calls = {"count": 0}

        def flaky_probe(instance):
            calls["count"] += 1
            return calls["count"] != 1  # 仅第一次失败

        self.system._probe = flaky_probe
        result = self.system.health_check(force=True)
        assert instance_id in result["failed"]
        instance = self.system.get_instance(instance_id)
        assert instance is not None  # 一次失败不被移除
        assert instance.consecutive_check_failures == 1
        self.system.health_check(force=True)
        assert self.system.get_instance(instance_id).consecutive_check_failures == 0

    # ---------------------------------------------------------- 服务注销

    def test_case_13(self):
        """注销后实例从注册列表移除，不再可被发现；重复注销返回 False。"""
        instance_id = self.system.register_service("mail", "10.0.5.1", 4001)
        assert self.system.service_deregister(instance_id) is True
        assert self.system.get_instance(instance_id) is None
        assert self.system.discover_service("mail") is None
        assert self.system.service_deregister(instance_id) is False

    def test_case_14(self):
        """注销时通知订阅方，依赖该服务的其他服务能收到下线事件。"""
        events = []
        self.system.subscribe("gateway", events.append)
        instance_id = self.system.register_service("gateway", "10.0.0.1", 8001)
        self.system.service_deregister(instance_id)
        assert len(events) == 1
        event = events[0]
        assert event["type"] == "deregister"
        assert event["service_name"] == "gateway"
        assert event["instance_id"] == instance_id

    def test_case_15(self):
        """注销时清理相关资源：发现缓存失效、连接被关闭并释放。"""

        class FakeConnection:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        instance_id = self.system.register_service("mail", "10.0.5.1", 4001)
        connection = FakeConnection()
        assert self.system.register_connection(instance_id, connection) is True
        self.system.discover_service("mail")  # 填充发现缓存
        assert "mail" in self.system._cache
        self.system.service_deregister(instance_id)
        assert connection.closed is True
        assert instance_id not in self.system._connections
        assert "mail" not in self.system._cache


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
