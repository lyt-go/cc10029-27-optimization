"""历史重放交付：取消与完成交错、重启恢复、按血缘取回与凭据消费的集成验证。"""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

import pytest

from app.services.exchange_replay import ExchangeReplayService
from app.services.transfer_lineage import TransferLineageService
from app.store import store


@pytest.fixture(autouse=True)
def reset_store():
    """每次测试重置内存 store；磁盘产物用唯一编号隔离。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _unique(prefix: str) -> str:
    return f"{prefix}{uuid.uuid4().hex[:6].upper()}"


def _write_batch(svc: ExchangeReplayService, batch_code: str, count: int) -> str:
    """写出原批产物文件，作为按原批血缘回查的来源。"""
    records = svc._simulate_records(batch_code, count=count, field_prefix="orig")
    path = svc._data_dir("batches") / f"{batch_code}.jsonl"
    svc._write_jsonl_atomic(path, records)
    return str(path)


def _prepare_replay(ha_cls, er_svc, batch_count: int = 2, per_batch: int = 4):
    """准备原批产物 + 归档卷 + 重放任务，返回（replay, archive_code, batches）。"""
    batches = [_unique("BATCH-") for _ in range(batch_count)]
    paths = {b: _write_batch(er_svc, b, per_batch) for b in batches}
    archive = ha_cls.create_archive(batches)
    ha_cls.execute_archive(archive["archive_id"])
    replay_range = f"{batches[0]}-{batches[-1]}"
    replay = er_svc.create_replay(archive["archive_code"], replay_range, "overwrite")
    return replay, archive["archive_code"], batches, paths


def _archive_cls():
    from app.services.history_archive import HistoryArchiveService
    return HistoryArchiveService()


class TestReplayDeliveryArbitration:
    """完成与取消在同一轮只能落下一个有效交付结论。"""

    def test_cancel_after_copy_written_wins_and_complete_is_ignored(self):
        er = ExchangeReplayService()
        replay, archive_code, batches, _ = _prepare_replay(_archive_cls(), er)

        executed = er.execute_replay(replay["replay_id"])
        assert executed["delivery_state"] == "copy_written"
        assert executed["conclusion"] is None
        assert executed["pending_operations"] == ["标记完成", "取消重放"]
        assert executed["records_readable"] > 0
        assert executed["shards"], "凭据必须写入实际可读分片明细"
        assert sum(s["record_count"] for s in executed["shards"]) == executed["records_readable"]

        cancelled = er.cancel_replay(replay["replay_id"], reason="目标端窗口关闭")
        assert cancelled["conclusion"] == "已取消"
        assert cancelled["status"] == "已取消"
        assert cancelled["pending_operations"] == []

        # 完成请求随后到达：本轮结论已落定，不得翻转
        completed = er.complete_replay(replay["replay_id"])
        assert completed["conclusion"] == "已取消"
        assert completed["status"] == "已取消"
        assert "已落定" in completed["message"]

        credential = er.get_delivery_credential(replay["replay_id"])
        assert credential["delivery_state"] == "cancelled"
        assert credential["conclusion"] == "已取消"
        assert credential["shards"] and credential["artifact_readable"] is True

    def test_complete_then_cancel_keeps_committed_version(self):
        er = ExchangeReplayService()
        replay, _, _, _ = _prepare_replay(_archive_cls(), er)
        er.execute_replay(replay["replay_id"])

        completed = er.complete_replay(replay["replay_id"])
        assert completed["conclusion"] == "已完成"
        assert completed["status"] == "已完成"
        committed_version = completed["credential_version"]

        cancelled = er.cancel_replay(replay["replay_id"])
        assert cancelled["conclusion"] == "已完成"
        assert cancelled["status"] == "已完成"
        assert "已落定" in cancelled["message"]

        credential = er.get_delivery_credential(replay["replay_id"])
        assert credential["credential_version"] == committed_version
        assert credential["delivery_state"] == "committed"

    def test_concurrent_complete_and_cancel_yield_single_conclusion(self):
        """完成与取消真正并发到达时，只有一个结论落定，另一侧返回原结论。"""
        import threading

        er = ExchangeReplayService()
        results: list[dict] = []
        errors: list[BaseException] = []

        for _ in range(6):
            replay, _, _, _ = _prepare_replay(_archive_cls(), er)
            er.execute_replay(replay["replay_id"])
            barrier = threading.Barrier(2)

            def run(fn):
                try:
                    barrier.wait(timeout=5)
                    results.append(fn())
                except BaseException as exc:  # pragma: no cover - 失败时暴露
                    errors.append(exc)

            workers = [
                threading.Thread(target=run, args=(lambda: er.complete_replay(replay["replay_id"]),)),
                threading.Thread(target=run, args=(lambda: er.cancel_replay(replay["replay_id"]),)),
            ]
            for w in workers:
                w.start()
            for w in workers:
                w.join(timeout=10)
            assert not errors

            credential = er.get_delivery_credential(replay["replay_id"])
            pair = results[-2:]
            assert len({r["conclusion"] for r in pair}) == 1
            assert pair[0]["conclusion"] == pair[1]["conclusion"] == credential["conclusion"]
            assert credential["credential_version"] == pair[0]["credential_version"]
            # 记录行状态与凭据结论一致：同一轮只有一个有效结论
            entry = er.get_entry(replay["replay_id"])
            assert entry["status"] == credential["conclusion"]
            assert entry["交付结论"] == credential["conclusion"]

    def test_generic_action_route_uses_same_arbitration(self):
        """通用动作接口的完成/取消也必须走交付仲裁。"""
        er = ExchangeReplayService()
        replay, _, _, _ = _prepare_replay(_archive_cls(), er)
        er.execute_replay(replay["replay_id"])

        entry, _ = er.run_action(replay["replay_id"], "取消重放")
        assert entry["status"] == "已取消"
        entry, message = er.run_action(replay["replay_id"], "标记完成")
        assert entry["status"] == "已取消"
        assert "已落定" in message

    def test_complete_refused_when_copy_unreadable(self):
        er = ExchangeReplayService()
        replay, _, _, _ = _prepare_replay(_archive_cls(), er)
        executed = er.execute_replay(replay["replay_id"])
        Path(executed["artifact_path"]).unlink()

        entry, message = er.run_action(replay["replay_id"], "标记完成")
        assert entry["status"] == "重放中"
        assert "不能落定完成结论" in message

    def test_credential_records_real_readable_shards_with_corruption(self):
        """分片明细必须反映实际可读内容，损坏行进入 unreadable_lines。"""
        er = ExchangeReplayService()
        replay, _, _, _ = _prepare_replay(_archive_cls(), er, batch_count=1, per_batch=7)
        executed = er.execute_replay(replay["replay_id"])
        artifact = Path(executed["artifact_path"])

        with open(artifact, "a", encoding="utf-8") as f:
            f.write("{not-json\n")
        with open(artifact, "a", encoding="utf-8") as f:
            f.write(json.dumps({"batch_key": "late", "line_no": 99}) + "\n")

        completed = er.complete_replay(replay["replay_id"])
        credential = er.get_delivery_credential(replay["replay_id"])
        assert completed["conclusion"] == "已完成"
        assert credential["unreadable_lines"] == [8]
        assert credential["record_count"] == 8
        assert credential["artifact_version"].endswith("/8")
        assert sum(s["record_count"] for s in credential["shards"]) == 8


class TestReplayRestartRecovery:
    """重启后已承诺数据仍可按版本与来源回查，重试保持幂等。"""

    def test_committed_credential_survives_restart_and_retry(self):
        er = ExchangeReplayService()
        replay, archive_code, batches, _ = _prepare_replay(_archive_cls(), er)
        executed = er.execute_replay(replay["replay_id"])
        completed = er.complete_replay(replay["replay_id"])
        committed_id = completed["credential_id"]
        artifact_version = completed["artifact_version"]

        # 模拟进程重启：内存表回到种子数据，磁盘凭据保留
        from app.seed import SEED_ROWS
        store._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        restarted = ExchangeReplayService()

        recovered = restarted.recover_delivery(replay["replay_code"])
        assert recovered["status"] == "已完成"
        assert recovered["credential_id"] == committed_id
        assert recovered["artifact_version"] == artifact_version
        assert recovered["rehydrated"] is True
        assert recovered["source"]["original_batches"] == batches
        assert recovered["shards"]

        # 重启后再次重试完成：不重复写副本、不生成新版本
        retried = restarted.complete_replay(replay_code=replay["replay_code"])
        assert retried["conclusion"] == "已完成"
        assert retried["credential_id"] == committed_id
        assert retried["credential_version"] == completed["credential_version"]

        # 重启后按顺序执行重放：直接返回已落定结论
        again = restarted.execute_replay(recovered["rehydrated_id"])
        assert again["recovered"] is True
        assert again["conclusion"] == "已完成"
        assert again["credential_id"] == committed_id

        retrieval = restarted.retrieve_by_lineage(replay_code=replay["replay_code"])
        assert retrieval["source"] == "replay_copy"
        assert retrieval["member_count"] == executed["records_readable"]
        assert retrieval["conclusion"] == "已完成"


class TestRetrievalByLineage:
    """后续取回：按原批血缘找到当时可用副本，并解释取消后还能取哪一份。"""

    def test_cancelled_replay_retrieves_original_batch_and_explains(self):
        er = ExchangeReplayService()
        replay, archive_code, batches, paths = _prepare_replay(_archive_cls(), er)
        executed = er.execute_replay(replay["replay_id"])
        er.cancel_replay(replay["replay_id"], reason="上游撤回")

        result = er.retrieve_by_lineage(batch_code=batches[0])
        assert result["source"] == "original_batch"
        assert result["member_count"] == 8
        assert result["final_status"] == "已取消"
        assert result["conclusion"] == "已取消"
        assert result["cancelled_copy"] is not None
        assert result["cancelled_copy"]["readable"] is True
        assert result["cancelled_copy"]["retrievable"] is False
        assert "已取消" in result["explanation"]
        assert "原批来源" in result["explanation"]

        kinds = {item["kind"]: item for item in result["available_copies"]}
        assert kinds["replay_copy"]["reason"].startswith("该轮交付结论为已取消")
        assert kinds["original_batch"]["retrievable"] is True

        for member in result["members"]:
            assert member["_source"] == "original_batch"
            assert member["batch_code"] in batches
            assert "_record_offset" in member

    def test_committed_replay_retrieves_replay_copy_with_shard_annotations(self):
        er = ExchangeReplayService()
        replay, _, batches, _ = _prepare_replay(_archive_cls(), er)
        er.execute_replay(replay["replay_id"])
        er.complete_replay(replay["replay_id"])

        result = er.retrieve_by_lineage(batch_code=batches[0])
        assert result["source"] == "replay_copy"
        assert result["member_count"] == 8
        assert result["final_status"] == "已完成"
        assert result["shards_returned"] == [0, 1]
        for member in result["members"]:
            assert member["_source"] == "replay_copy"
            assert member["_shard_index"] in (0, 1)
            assert member["_line_no"] is not None

    def test_queued_retrieval_drains_after_conclusion(self):
        """另一个取回请求在完成/取消期间排队，重启后继续并按当时结论返回。"""
        er = ExchangeReplayService()
        replay, _, batches, _ = _prepare_replay(_archive_cls(), er)
        er.execute_replay(replay["replay_id"])

        queued = er.retrieve_by_lineage(batch_code=batches[0], queue=True)
        assert queued["status"] == "queued"
        assert len(er.list_queued_retrievals()) == 1

        # 取消先落定，之后重启并排空队列
        er.cancel_replay(replay["replay_id"], reason="取消优先")
        from app.seed import SEED_ROWS
        store._tables = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        restarted = ExchangeReplayService()
        drained = restarted.drain_retrievals()
        assert len(drained) == 1
        assert drained[0]["request_id"] == queued["request_id"]
        assert drained[0]["source"] == "original_batch"
        assert drained[0]["final_status"] == "已取消"
        assert drained[0]["member_count"] == 8
        assert restarted.list_queued_retrievals() == []

    def test_queued_retrieval_after_complete_uses_replay_copy(self):
        er = ExchangeReplayService()
        replay, _, batches, _ = _prepare_replay(_archive_cls(), er)
        er.execute_replay(replay["replay_id"])
        queued = er.retrieve_by_lineage(replay_code=replay["replay_code"], queue=True)
        er.complete_replay(replay["replay_id"])

        drained = er.drain_retrievals()
        assert drained[0]["request_id"] == queued["request_id"]
        assert drained[0]["source"] == "replay_copy"
        assert drained[0]["final_status"] == "已完成"


class TestLineageConsumesReplayCredential:
    """传输血缘消费重放交付血缘凭据。"""

    def test_lineage_records_credential_version_source_and_shards(self):
        er = ExchangeReplayService()
        tl = TransferLineageService()
        replay, archive_code, batches, _ = _prepare_replay(_archive_cls(), er, batch_count=1)
        executed = er.execute_replay(replay["replay_id"])
        completed = er.complete_replay(replay["replay_id"])

        result = tl.record_lineage(batches[0], True)
        assert result["replay_credential_id"] == completed["credential_id"]
        assert result["replay_delivery_state"] == "committed"
        assert result["replay_shards"] == len(executed["shards"])

        lineage_files = sorted(
            tl._data_dir("lineage").glob(f"*{result['lineage_code']}*.jsonl"))
        assert lineage_files
        payload = tl._read_jsonl(lineage_files[-1])[0]
        consumed = payload["replay_delivery"]
        assert consumed["consumed"] is True
        assert consumed["credential_version"] == completed["credential_version"]
        assert consumed["credit_version"] == f"replay-v{completed['credential_version']}"
        assert consumed["delivery_state"] == "committed"
        assert consumed["source"]["original_batches"] == batches
        assert consumed["shards"] == executed["shards"]

    def test_lineage_without_replay_credential_still_records(self):
        tl = TransferLineageService()
        result = tl.record_lineage(_unique("PLAIN-"), True)
        assert result["replay_credential_id"] is None
        assert result["replay_shards"] == 0
