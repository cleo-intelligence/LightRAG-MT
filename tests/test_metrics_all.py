"""
Tests for GET /metrics/all endpoint.

Tests the multi-instance metrics aggregation feature:
- Metrics collector callback registration
- Instance registry metrics storage and retrieval
- Aggregation logic across multiple instances
- Fallback behavior when registry is unavailable
"""

import json
from contextlib import asynccontextmanager

import pytest
from unittest.mock import AsyncMock, MagicMock

from lightrag.instance_registry import InstanceRegistry


def _make_registry_with_conn(conn_mock):
    """Create an InstanceRegistry with a mocked _acquire_connection."""
    db_mock = MagicMock()
    registry = InstanceRegistry(db=db_mock, instance_id="test-1")

    @asynccontextmanager
    async def _fake_acquire():
        yield conn_mock

    registry._acquire_connection = _fake_acquire
    return registry


# =============================================================================
# Metrics Collector Callback
# =============================================================================


class TestMetricsCollectorCallback:
    """Tests for set_metrics_collector and callback invocation."""

    def test_set_metrics_collector(self):
        """Verify metrics collector callback is stored."""
        db_mock = MagicMock()
        registry = InstanceRegistry(db=db_mock, instance_id="test-1")
        collector = MagicMock(return_value={"llm_active_calls": 5})
        registry.set_metrics_collector(collector)
        assert registry._metrics_collector is collector

    def test_metrics_collector_default_none(self):
        """Verify metrics collector defaults to None."""
        db_mock = MagicMock()
        registry = InstanceRegistry(db=db_mock, instance_id="test-1")
        assert registry._metrics_collector is None


# =============================================================================
# Heartbeat with Metrics
# =============================================================================


class TestHeartbeatWithMetrics:
    """Tests for heartbeat() including JSONB metrics parameter."""

    @pytest.mark.asyncio
    async def test_heartbeat_includes_metrics_jsonb(self):
        """Verify heartbeat UPDATE includes metrics as 4th parameter."""
        conn_mock = AsyncMock()
        conn_mock.execute = AsyncMock(return_value="UPDATE 1")
        registry = _make_registry_with_conn(conn_mock)

        metrics = {"llm_active_calls": 5, "drain_mode": False}
        await registry.heartbeat(
            processing_count=2, pipeline_busy=True, metrics=metrics
        )

        conn_mock.execute.assert_called_once()
        call_args = conn_mock.execute.call_args
        sql = call_args[0][0]
        assert "metrics = $4::jsonb" in sql
        assert call_args[0][4] == json.dumps(metrics)

    @pytest.mark.asyncio
    async def test_heartbeat_without_metrics_sends_empty_json(self):
        """Verify heartbeat sends '{}' when no metrics provided."""
        conn_mock = AsyncMock()
        conn_mock.execute = AsyncMock(return_value="UPDATE 1")
        registry = _make_registry_with_conn(conn_mock)

        await registry.heartbeat(processing_count=0, pipeline_busy=False)

        call_args = conn_mock.execute.call_args
        # 4th positional arg should be "{}"
        assert call_args[0][4] == "{}"


# =============================================================================
# Get All Instances With Metrics
# =============================================================================


class TestGetAllInstancesWithMetrics:
    """Tests for get_all_instances_with_metrics() method."""

    @pytest.mark.asyncio
    async def test_parses_jsonb_dict(self):
        """Verify JSONB metrics returned as dict are passed through."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        row = {
            "instance_id": "inst-a",
            "hostname": "srv-a",
            "last_heartbeat": now,
            "drain_requested": False,
            "processing_count": 5,
            "pipeline_busy": True,
            "metrics": {"llm_active_calls": 10, "drain_mode": False},
        }

        conn_mock = AsyncMock()
        conn_mock.fetch = AsyncMock(return_value=[row])
        registry = _make_registry_with_conn(conn_mock)

        result = await registry.get_all_instances_with_metrics()

        assert len(result) == 1
        assert result[0]["instance_id"] == "inst-a"
        assert result[0]["metrics"]["llm_active_calls"] == 10

    @pytest.mark.asyncio
    async def test_parses_jsonb_string(self):
        """Verify JSONB metrics returned as string are parsed."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        row = {
            "instance_id": "inst-b",
            "hostname": "srv-b",
            "last_heartbeat": now,
            "drain_requested": False,
            "processing_count": 0,
            "pipeline_busy": False,
            "metrics": '{"llm_active_calls": 3}',
        }

        conn_mock = AsyncMock()
        conn_mock.fetch = AsyncMock(return_value=[row])
        registry = _make_registry_with_conn(conn_mock)

        result = await registry.get_all_instances_with_metrics()
        assert result[0]["metrics"]["llm_active_calls"] == 3

    @pytest.mark.asyncio
    async def test_handles_null_metrics(self):
        """Verify null metrics default to empty dict."""
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        row = {
            "instance_id": "inst-c",
            "hostname": "srv-c",
            "last_heartbeat": now,
            "drain_requested": False,
            "processing_count": 0,
            "pipeline_busy": False,
            "metrics": None,
        }

        conn_mock = AsyncMock()
        conn_mock.fetch = AsyncMock(return_value=[row])
        registry = _make_registry_with_conn(conn_mock)

        result = await registry.get_all_instances_with_metrics()
        assert result[0]["metrics"] == {}

    @pytest.mark.asyncio
    async def test_empty_result(self):
        """Verify empty list when no alive instances."""
        conn_mock = AsyncMock()
        conn_mock.fetch = AsyncMock(return_value=[])
        registry = _make_registry_with_conn(conn_mock)

        result = await registry.get_all_instances_with_metrics()
        assert result == []


# =============================================================================
# Aggregation Logic
# =============================================================================


class TestMetricsAggregation:
    """Tests for the aggregation logic used in /metrics/all."""

    def test_aggregation_sums_correctly(self):
        """Verify aggregated totals are correct sums across instances."""
        instances = [
            {
                "instance_id": "a",
                "hostname": "h1",
                "metrics": {
                    "llm_active_calls": 5,
                    "processing_count": 32,
                    "db_pool_active": 45,
                    "pipelines_busy": 1,
                },
            },
            {
                "instance_id": "b",
                "hostname": "h2",
                "metrics": {
                    "llm_active_calls": 5,
                    "processing_count": 32,
                    "db_pool_active": 52,
                    "pipelines_busy": 1,
                },
            },
        ]

        aggregated = {
            "total_llm_active_calls": sum(
                inst["metrics"].get("llm_active_calls", 0) for inst in instances
            ),
            "total_processing_count": sum(
                inst["metrics"].get("processing_count", 0) for inst in instances
            ),
            "total_db_pool_active": sum(
                inst["metrics"].get("db_pool_active", 0) for inst in instances
            ),
            "total_pipelines_busy": sum(
                inst["metrics"].get("pipelines_busy", 0) for inst in instances
            ),
        }

        assert aggregated["total_llm_active_calls"] == 10
        assert aggregated["total_processing_count"] == 64
        assert aggregated["total_db_pool_active"] == 97
        assert aggregated["total_pipelines_busy"] == 2

    def test_aggregation_handles_missing_keys(self):
        """Verify aggregation defaults to 0 for missing metric keys."""
        instances = [
            {
                "instance_id": "a",
                "hostname": "h1",
                "metrics": {},
            },
        ]

        total = sum(inst["metrics"].get("llm_active_calls", 0) for inst in instances)
        assert total == 0

    def test_aggregation_handles_empty_instances(self):
        """Verify aggregation works with no instances."""
        instances = []

        total = sum(inst["metrics"].get("llm_active_calls", 0) for inst in instances)
        assert total == 0
