"""历史重放领域服务：维护归档批次按范围重放回目标端的重放速率与覆盖方式控制。

本模块读取归档产物文件，按范围回放到目标端，支持增量追加和全量覆盖两种模式，
重放完成后写入重放结果文件。

重放交付在本模块中按"一轮交付一个结论"处理：目标副本写出后，完成请求与取消请求
可能同时到达，也可能在上游排队了按原批次血缘的取回请求。因此每次写出副本都会生成
一份**重放交付血缘凭据**，把当时实际可读的分片明细（行号区间、条数、校验值）连同
原批来源一起固化下来；完成/取消通过交付锁与版本化凭据仲裁，同一轮只落一个有效结论。
凭据落盘为版本化 JSON，重启后仍可按版本与来源回查，供传输血缘消费和后续取回使用。
"""
from __future__ import annotations

import json
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

SHARD_SIZE = 5
DELIVERABLE_STATES = ("committed",)


class ExchangeReplayService(BaseModuleService):
    MODULE = "exchange_replay"
    ENTITY = "重放任务"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["重放编号", "归档编号", "重放范围", "起始批次", "截止批次", "重放速率", "覆盖方式", "重放结果"]
    REQUIRED_FIELDS = ["重放编号", "归档编号", "重放范围"]
    STATUS_ORDER = ["草稿", "待重放", "重放中", "已完成", "已取消"]
    ACTION_RULES = {"启动重放": "重放中", "标记完成": "已完成", "取消重放": "已取消"}
    NEGATIVE_ACTIONS = ["取消重放"]

    # 两个待确认操作：完成与取消。同一轮交付只允许其中一个落定。
    PENDING_DELIVERY_OPERATIONS = ["标记完成", "取消重放"]

    # -- 交付锁：同一重放任务的完成/取消仲裁串行化 --

    _lock_guard = threading.Lock()
    _task_locks: dict[str, threading.Lock] = {}

    @classmethod
    def _delivery_lock(cls, replay_code: str) -> threading.Lock:
        with cls._lock_guard:
            return cls._task_locks.setdefault(replay_code, threading.Lock())

    # -- 对外动作：完成/取消走交付仲裁，其余沿用基类状态流转 --

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action in self.PENDING_DELIVERY_OPERATIONS:
            return self._conclude_delivery(entry_id=entry_id, action=action)
        return super().run_action(entry_id, action)

    # -- 任务创建与重放执行 --

    def create_replay(self, archive_code: str, replay_range: str, mode: str = "append") -> dict[str, Any]:
        """创建历史重放任务：指定归档卷、重放范围和覆盖方式。"""
        replay_code = f"EXCH-{archive_code}-{int(time.time())}"
        suffix = 1
        while self._delivery_dir_state_exists(replay_code) or any(
            row.get("重放编号") == replay_code for row in store.rows(self.MODULE)
        ):
            suffix += 1
            replay_code = f"EXCH-{archive_code}-{int(time.time())}-{suffix}"
        values = {
            "重放编号": replay_code,
            "归档编号": archive_code,
            "重放范围": replay_range,
            "起始批次": replay_range.split("-")[0] if "-" in replay_range else replay_range,
            "截止批次": replay_range.split("-")[-1] if "-" in replay_range else replay_range,
            "重放速率": "500",
            "覆盖方式": mode,
            "重放结果": "待重放",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建重放任务失败: {missing}")
        self._write_jsonl(
            self._data_dir("events") / "replay_log.jsonl",
            [{"action": "create_replay", "replay_code": replay_code, "archive_code": archive_code,
              "replay_range": replay_range, "mode": mode, "at": time.time()}]
        )
        return {"replay_code": replay_code, "replay_id": entry["id"],
                "archive_code": archive_code, "mode": mode}

    def execute_replay(self, replay_id: int, request_ref: str = "", force_new_round: bool = False) -> dict[str, Any]:
        """执行历史重放：读取归档产物，按覆盖方式写出目标副本并生成交付血缘凭据。

        写出副本后立即回读磁盘，把**实际可读**的分片明细写入凭据，并把「标记完成 /
        取消重放」登记为待确认操作。若该轮已有交付结论（包括重启后重试），直接返回
        已落定的结论，不再重复写出副本，也不再翻转状态。
        """
        replay_entry = self.get_entry(replay_id)
        if replay_entry is None:
            return {"error": f"重放任务 {replay_id} 不存在"}
        replay_code = replay_entry["重放编号"]

        with self._delivery_lock(replay_code):
            state = self._load_state(replay_code)
            if state and state.get("conclusion") and not force_new_round:
                credential = self._load_credential(replay_code) or {}
                return self._recovered_result(replay_entry, state, credential, recovered=True)

            entry, msg = super().run_action(replay_id, "启动重放")
            if entry is None:
                return {"error": msg, "replay_id": replay_id}

            archive_code = replay_entry["归档编号"]
            replay_range = replay_entry["重放范围"]
            mode = replay_entry["覆盖方式"]

            archive_records, archive_artifact = self._read_archive_records(archive_code)
            if not archive_records:
                archive_records = self._simulate_records(archive_code, count=10, field_prefix="replay")

            # 写入目标重放副本
            replay_file = self._data_dir("replay") / f"{replay_code}.jsonl"
            if mode == "overwrite":
                combined = list(archive_records)
            else:
                combined = self._read_jsonl(replay_file) + list(archive_records)
            written = self._write_jsonl_atomic(replay_file, combined)

            # 回读实际落盘内容，生成可读分片明细
            inspection = self._inspect_artifact(replay_file, SHARD_SIZE)
            source = self._build_source(replay_entry, archive_artifact)

            prior_state = state or {}
            round_no = int(prior_state.get("round", 0)) + 1 \
                if (force_new_round or not prior_state) else int(prior_state["round"])
            state = {
                "replay_code": replay_code,
                "round": round_no,
                "conclusion": None,
                "conclusion_action": None,
                "conclusion_at": None,
                "pending_operations": list(self.PENDING_DELIVERY_OPERATIONS),
                "credential_version": int(prior_state.get("credential_version", 0)),
                "credential_id": prior_state.get("credential_id"),
                "artifact_path": str(replay_file),
                "artifact_state": "copy_written",
                "request_ref": request_ref,
                "history": list(prior_state.get("history", [])),
            }
            credential = self._build_credential(
                replay_entry, source=source, artifact_path=str(replay_file),
                inspection=inspection, delivery_state="copy_written",
                conclusion=None, round_no=round_no,
            )
            credential = self._persist_credential(replay_code, state, credential)

            replay_entry["重放结果"] = f"已写出{written}条记录，待确认交付"
            replay_entry["交付凭据"] = credential["credential_id"]
            replay_entry["交付版本"] = str(credential["credential_version"])
            replay_entry["交付状态"] = "copy_written"
            self._save_state(replay_code, state)

            return {
                "replay_code": replay_code,
                "archive_code": archive_code,
                "mode": mode,
                "records_replayed": written,
                "records_readable": inspection["record_count"],
                "artifact_path": str(replay_file),
                "credential_id": credential["credential_id"],
                "credential_version": credential["credential_version"],
                "delivery_state": "copy_written",
                "shards": inspection["shards"],
                "unreadable_lines": inspection["unreadable_lines"],
                "pending_operations": list(state["pending_operations"]),
                "conclusion": None,
            }

    # -- 交付结论仲裁 --

    def complete_replay(
        self,
        replay_id: int | None = None,
        *,
        replay_code: str | None = None,
        request_ref: str = "",
    ) -> dict[str, Any]:
        """完成请求入口：按重放编号（重启后可用）或记录 id 落定完成结论。"""
        code = replay_code or self._code_for_id(replay_id)
        if not code:
            return {"error": f"重放任务 {replay_id} 不存在"}
        entry_id = self._entry_id_for_code(code, replay_id)
        if entry_id is None:
            recovered = self.recover_delivery(code)
            if "error" in recovered:
                return recovered
            entry_id = recovered["rehydrated_id"]
        entry, message = self._conclude_delivery(
            entry_id=entry_id, action="标记完成", request_ref=request_ref)
        if entry is None:
            return {"error": message, "replay_code": code}
        return self._conclusion_result(code, entry, message)

    def cancel_replay(
        self,
        replay_id: int | None = None,
        reason: str = "",
        *,
        replay_code: str | None = None,
        request_ref: str = "",
    ) -> dict[str, Any]:
        """取消请求入口：副本已写出但未确认时发出取消，取消可先落定，完成随后被拒。"""
        code = replay_code or self._code_for_id(replay_id)
        if not code:
            return {"error": f"重放任务 {replay_id} 不存在"}
        entry_id = self._entry_id_for_code(code, replay_id)
        if entry_id is None:
            recovered = self.recover_delivery(code)
            if "error" in recovered:
                return recovered
            entry_id = recovered["rehydrated_id"]
        entry, message = self._conclude_delivery(
            entry_id=entry_id, action="取消重放", reason=reason, request_ref=request_ref)
        if entry is None:
            return {"error": message, "replay_code": code}
        return self._conclusion_result(code, entry, message)

    def _conclude_delivery(
        self,
        *,
        entry_id: int,
        action: str,
        reason: str = "",
        request_ref: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """在交付锁内为一个重放任务落定本轮唯一有效交付结论。

        - 本轮已有结论：返回原结论，不覆盖、不翻转（重启后重试同样幂等）。
        - 完成为可读副本建立 committed 版本；副本不可读则拒绝完成。
        - 取消把该副本标记为不可交付，但保留可读分片明细，供取回时解释。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        replay_code = entry["重放编号"]

        with self._delivery_lock(replay_code):
            state = self._load_state(replay_code) or self._bootstrap_state(entry)
            if state.get("conclusion"):
                self._project_state(entry, state)
                return entry, (
                    f"本轮交付结论已落定：{state['conclusion']}，"
                    f"忽略重复的{action}（凭据 {state.get('credential_id')}）"
                )

            credential = self._load_credential(replay_code)
            if credential is None:
                credential = self._build_credential(
                    entry,
                    source=self._build_source(entry, None),
                    artifact_path=state.get("artifact_path"),
                    inspection={"records": [], "lines": [], "shards": [], "unreadable_lines": [],
                                "record_count": 0, "checksum": "", "readable": False},
                    delivery_state="copy_written" if state.get("artifact_path") else "no_copy",
                    conclusion=None,
                    round_no=int(state.get("round", 1)),
                )
            else:
                # 结论落定前重新回读副本，凭据只承诺此刻实际可读的分片
                artifact_path = Path(credential.get("artifact_path") or "")
                inspection = self._inspect_artifact(artifact_path, SHARD_SIZE) \
                    if credential.get("artifact_path") else self._empty_inspection()
                credential["artifact_readable"] = bool(inspection["readable"])
                credential["record_count"] = int(inspection["record_count"])
                credential["shards"] = inspection["shards"]
                credential["unreadable_lines"] = inspection["unreadable_lines"]
                credential["artifact_checksum"] = inspection["checksum"]
                credential["artifact_version"] = self._artifact_version(inspection)

            previous_id = credential.get("credential_id")
            now = time.time()
            if action == "标记完成":
                if not credential.get("artifact_readable") or not credential.get("shards"):
                    return entry, (
                        f"重放副本无实际可读分片，不能落定完成结论（凭据 {previous_id}）"
                    )
                credential["delivery_state"] = "committed"
                credential["conclusion"] = "已完成"
                credential["committed_at"] = now
                if not self._state_status_is_terminal(entry, "已完成"):
                    super().run_action(entry_id, "标记完成")
                message = f"重放交付已完成，凭据 {previous_id} 已承诺"
            else:
                credential["delivery_state"] = "cancelled"
                credential["conclusion"] = "已取消"
                credential["cancelled_at"] = now
                credential["cancel_reason"] = reason
                if not self._state_status_is_terminal(entry, "已取消"):
                    super().run_action(entry_id, "取消重放")
                message = f"重放交付已取消，原批来源仍可回查（凭据 {previous_id}）"

            credential["concluded_at"] = now
            credential["round"] = int(state.get("round", 1))
            credential = self._persist_credential(replay_code, state, credential)

            state["conclusion"] = credential["conclusion"]
            state["conclusion_action"] = action
            state["conclusion_at"] = now
            state["pending_operations"] = []
            state["artifact_state"] = credential["delivery_state"]
            state["credential_id"] = credential["credential_id"]
            state["history"].append({
                "round": state["round"], "action": action,
                "conclusion": credential["conclusion"],
                "credential_id": credential["credential_id"],
                "credential_version": credential["credential_version"],
                "request_ref": request_ref, "reason": reason, "at": now,
            })
            self._save_state(replay_code, state)
            self._project_state(entry, state)
            self._write_jsonl(
                self._data_dir("events") / "replay_delivery_log.jsonl",
                [{"action": action, "replay_code": replay_code,
                  "conclusion": credential["conclusion"], "round": state["round"],
                  "credential_id": credential["credential_id"],
                  "credential_version": credential["credential_version"],
                  "request_ref": request_ref, "reason": reason, "at": now}]
            )
            return entry, message

    # -- 凭据读取 / 重启恢复 --

    def get_delivery_credential(
        self,
        replay_id: int | None = None,
        *,
        replay_code: str | None = None,
    ) -> dict[str, Any] | None:
        """读取重放交付血缘凭据（含版本、可读分片明细与来源），不依赖内存 store。"""
        code = replay_code or self._code_for_id(replay_id)
        if not code:
            return None
        credential = self._load_credential(code)
        if credential is None:
            return None
        state = self._load_state(code) or {}
        credential["round"] = int(state.get("round", credential.get("round", 1)))
        credential["pending_operations"] = list(state.get("pending_operations", []))
        return credential

    def recover_delivery(self, replay_code: str) -> dict[str, Any]:
        """重启恢复：从磁盘凭据重建任务行，返回当时承诺的版本与来源。"""
        credential = self._load_credential(replay_code)
        state = self._load_state(replay_code)
        if credential is None and state is None:
            return {"error": f"重放任务 {replay_code} 无持久化交付凭据"}

        credential = credential or {}
        state = state or {}
        entry = self._find_row_by_code(replay_code)
        rehydrated = False
        if entry is None:
            rows = store.rows(self.MODULE)
            entry = {
                "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                "重放编号": replay_code,
                "归档编号": credential.get("archive_code") or state.get("archive_code", ""),
                "重放范围": credential.get("replay_range", ""),
                "起始批次": "",
                "截止批次": "",
                "重放速率": "500",
                "覆盖方式": credential.get("mode", "append"),
                "重放结果": credential.get("conclusion") or state.get("artifact_state", "待确认"),
                "status": self._status_for_conclusion(credential.get("conclusion") or state.get("conclusion")),
                "pending": not bool(credential.get("conclusion") or state.get("conclusion")),
                "abnormal": (credential.get("conclusion") or state.get("conclusion")) == "已取消",
            }
            rows.append(entry)
            rehydrated = True
        self._project_state(entry, {**state, **credential})
        return {
            "replay_code": replay_code,
            "rehydrated": rehydrated,
            "rehydrated_id": entry["id"],
            "status": entry.get("status"),
            "credential_id": credential.get("credential_id") or state.get("credential_id"),
            "credential_version": credential.get("credential_version")
            or state.get("credential_version"),
            "delivery_state": credential.get("delivery_state") or state.get("artifact_state"),
            "conclusion": credential.get("conclusion") or state.get("conclusion"),
            "source": credential.get("source", {}),
            "artifact_path": credential.get("artifact_path"),
            "artifact_version": credential.get("artifact_version"),
            "shards": credential.get("shards", []),
            "round": int(state.get("round", credential.get("round", 1))),
        }

    def find_credential_by_batch(self, batch_code: str) -> dict[str, Any] | None:
        """按原批血缘查找最新交付凭据（跨重放任务），供传输血缘消费。"""
        return self._resolve_credential(batch_code=batch_code, replay_code="", archive_code="")

    # -- 后续取回：按原批次血缘找到当时可用副本 --

    def retrieve_by_lineage(
        self,
        *,
        batch_code: str = "",
        replay_code: str = "",
        archive_code: str = "",
        request_ref: str = "",
        queue: bool = False,
    ) -> dict[str, Any]:
        """按原批次血缘取回当时可用的原批或重放副本，并解释取消后还能取哪一份。

        返回的 members 来自实际回读成功的分片，附带分片序号与来源标注；同时给出
        可读副本清单、交付结论和最终状态，便于调用方核对成员、来源与结论。
        """
        if queue:
            return self.enqueue_retrieval(
                batch_code=batch_code, replay_code=replay_code,
                archive_code=archive_code, request_ref=request_ref,
            )

        credential = self._resolve_credential(
            batch_code=batch_code, replay_code=replay_code, archive_code=archive_code)
        available: list[dict[str, Any]] = []
        explanation_parts: list[str] = []

        # 1) 重放副本候选
        if credential is not None:
            artifact_path = credential.get("artifact_path") or ""
            artifact_inspect = self._inspect_artifact(Path(artifact_path), SHARD_SIZE) \
                if artifact_path else self._empty_inspection()
            retrievable = (
                bool(credential.get("artifact_readable"))
                and credential.get("delivery_state") in DELIVERABLE_STATES
            )
            reason = (
                "该轮交付结论为已完成，副本可作为交付来源"
                if retrievable else
                f"该轮交付结论为{credential.get('conclusion') or '未定论'}，副本不作为交付来源"
            )
            available.append({
                "kind": "replay_copy",
                "replay_code": credential.get("replay_code"),
                "path": artifact_path,
                "readable": bool(credential.get("artifact_readable")),
                "record_count": int(credential.get("record_count") or 0),
                "shard_count": len(credential.get("shards") or []),
                "retrievable": retrievable,
                "reason": reason,
                "credential_id": credential.get("credential_id"),
                "credential_version": credential.get("credential_version"),
            })
            if credential.get("conclusion") == "已取消":
                explanation_parts.append(
                    "重放副本存在且可读，但本轮交付结论为已取消，不能作为交付来源；"
                    "取回改用原批来源。"
                )
            elif retrievable:
                explanation_parts.append("重放副本已完成交付，取回使用重放副本。")
            else:
                explanation_parts.append("重放副本未完成交付，取回使用原批来源。")
            members, source_kind, source_detail = self._members_from_replay(
                credential, artifact_inspect)
            if retrievable and members:
                for m in members:
                    m["_source"] = "replay_copy"
                return self._retrieval_result(
                    status=credential.get("conclusion") or "未定论",
                    source_kind="replay_copy",
                    source_detail=source_detail,
                    members=members,
                    available=available,
                    credential=credential,
                    explanation=" ".join(explanation_parts),
                    request_ref=request_ref,
                )
        else:
            explanation_parts.append("未找到该原批的重放交付凭据，取回改用原批来源。")

        # 2) 原批 / 归档来源
        batches = self._resolve_batches(
            credential=credential, batch_code=batch_code, archive_code=archive_code)
        original_members, source_kind, source_detail = self._members_from_original(
            batches, archive_code=archive_code or (credential or {}).get("archive_code", ""))
        available.append({
            "kind": source_kind,
            "path": source_detail.get("path", ""),
            "readable": bool(original_members),
            "record_count": len(original_members),
            "retrievable": bool(original_members),
            "reason": "原批来源可直接回查" if original_members else "原批产物不可读或缺失",
            "batches": batches,
        })
        if not original_members:
            explanation_parts.append("原批来源也没有可读产物，本次取回无可用成员。")
        else:
            explanation_parts.append(
                f"原批来源仍可回查，取回 {len(original_members)} 条记录。")

        return self._retrieval_result(
            status=(credential or {}).get("conclusion") or "无有效重放交付",
            source_kind=source_kind,
            source_detail=source_detail,
            members=original_members,
            available=available,
            credential=credential,
            explanation=" ".join(explanation_parts),
            request_ref=request_ref,
        )

    # -- 取回请求排队（可在重启后继续） --

    def enqueue_retrieval(
        self,
        *,
        batch_code: str = "",
        replay_code: str = "",
        archive_code: str = "",
        request_ref: str = "",
    ) -> dict[str, Any]:
        """把按原批次血缘的取回请求登记到磁盘队列，返回排队凭据。"""
        request_id = f"RETR-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        payload = {
            "request_id": request_id,
            "request_ref": request_ref,
            "batch_code": batch_code,
            "replay_code": replay_code,
            "archive_code": archive_code,
            "status": "queued",
            "attempts": 0,
            "created_at": time.time(),
        }
        self._write_json_atomic(self._retrieval_dir() / f"{request_id}.json", payload)
        return {"queued": True, "request_id": request_id, "status": "queued",
                "batch_code": batch_code, "replay_code": replay_code}

    def drain_retrievals(self) -> list[dict[str, Any]]:
        """处理所有排队中的取回请求；重启后调用可从磁盘继续未完成请求。"""
        results = []
        for path in sorted(self._retrieval_dir().glob("*.json")):
            payload = self._read_json_file(path)
            if not payload or payload.get("status") != "queued":
                continue
            payload["attempts"] = int(payload.get("attempts", 0)) + 1
            result = self.retrieve_by_lineage(
                batch_code=payload.get("batch_code", ""),
                replay_code=payload.get("replay_code", ""),
                archive_code=payload.get("archive_code", ""),
                request_ref=payload.get("request_ref", ""),
            )
            payload["status"] = "done"
            payload["result"] = result
            payload["finished_at"] = time.time()
            self._write_json_atomic(path, payload)
            results.append({
                "request_id": payload["request_id"],
                "status": payload["status"],
                "attempts": payload["attempts"],
                "source": result["source"],
                "member_count": result["member_count"],
                "final_status": result["final_status"],
            })
        return results

    def list_queued_retrievals(self) -> list[dict[str, Any]]:
        """列出仍在队列中的取回请求，便于重启后核对。"""
        queued = []
        for path in sorted(self._retrieval_dir().glob("*.json")):
            payload = self._read_json_file(path)
            if payload and payload.get("status") == "queued":
                queued.append(payload)
        return queued

    # -- 凭据构造 --

    def _build_credential(
        self,
        entry: dict[str, Any],
        *,
        source: dict[str, Any],
        artifact_path: str | None,
        inspection: dict[str, Any],
        delivery_state: str,
        conclusion: str | None,
        round_no: int,
    ) -> dict[str, Any]:
        return {
            "credential_id": None,
            "credential_version": 0,
            "kind": "replay_delivery_lineage_credential",
            "replay_code": entry["重放编号"],
            "replay_id": entry.get("id"),
            "archive_code": entry.get("归档编号", ""),
            "replay_range": entry.get("重放范围", ""),
            "mode": entry.get("覆盖方式", "append"),
            "round": round_no,
            "delivery_state": delivery_state,
            "conclusion": conclusion,
            "pending_operations": list(self.PENDING_DELIVERY_OPERATIONS),
            "source": source,
            "artifact_path": artifact_path,
            "artifact_readable": bool(inspection.get("readable")),
            "artifact_version": self._artifact_version(inspection),
            "artifact_checksum": inspection.get("checksum", ""),
            "record_count": int(inspection.get("record_count", 0)),
            "shards": inspection.get("shards", []),
            "unreadable_lines": inspection.get("unreadable_lines", []),
            "written_at": time.time(),
            "concluded_at": None,
            "committed_at": None,
            "cancelled_at": None,
            "cancel_reason": "",
            "supersedes": None,
        }

    def _persist_credential(
        self, replay_code: str, state: dict[str, Any], credential: dict[str, Any]
    ) -> dict[str, Any]:
        """版本化落盘凭据：写新版本文件 + 更新索引 + 追加交付日志。"""
        previous_id = state.get("credential_id")
        version = int(state.get("credential_version", 0)) + 1
        credential["credential_version"] = version
        credential["credential_id"] = f"RDLV-{replay_code}-v{version}"
        credential["supersedes"] = previous_id if previous_id != credential["credential_id"] else None
        credential["persisted_at"] = time.time()
        directory = self._delivery_dir()
        self._write_json_atomic(
            directory / f"{replay_code}.credential.v{version}.json", credential)
        self._write_json_atomic(
            directory / f"{replay_code}.credential.json", credential)
        state["credential_version"] = version
        state["credential_id"] = credential["credential_id"]
        self._write_jsonl(
            self._data_dir("lineage") / "replay_delivery_log.jsonl",
            [{"action": "persist_credential", "replay_code": replay_code,
              "credential_id": credential["credential_id"],
              "credential_version": version,
              "delivery_state": credential["delivery_state"],
              "conclusion": credential.get("conclusion"),
              "record_count": credential.get("record_count"),
              "artifact_version": credential.get("artifact_version"),
              "at": credential["persisted_at"]}]
        )
        return credential

    # -- 磁盘读取工具 --

    def _load_credential(self, replay_code: str) -> dict[str, Any] | None:
        return self._read_json_file(self._delivery_dir() / f"{replay_code}.credential.json")

    def _load_state(self, replay_code: str) -> dict[str, Any] | None:
        state = self._read_json_file(self._delivery_dir() / f"{replay_code}.state.json")
        if state is None:
            return None
        credential = self._load_credential(replay_code)
        # 凭据可能比状态先落盘（写完凭据后重启）：用凭据补齐结论，保证可回查。
        if credential and credential.get("conclusion") and not state.get("conclusion"):
            state["conclusion"] = credential["conclusion"]
            state["artifact_state"] = credential.get("delivery_state")
            state["credential_id"] = credential.get("credential_id")
            state["credential_version"] = max(
                int(state.get("credential_version", 0)),
                int(credential.get("credential_version", 0)))
            state["pending_operations"] = []
        return state

    def _save_state(self, replay_code: str, state: dict[str, Any]) -> None:
        state["updated_at"] = time.time()
        self._write_json_atomic(self._delivery_dir() / f"{replay_code}.state.json", state)

    def _bootstrap_state(self, entry: dict[str, Any]) -> dict[str, Any]:
        """没有执行过重放就收到取消/完成时，建立第 1 轮待确认状态。"""
        return {
            "replay_code": entry["重放编号"],
            "round": 1,
            "conclusion": None,
            "conclusion_action": None,
            "conclusion_at": None,
            "pending_operations": list(self.PENDING_DELIVERY_OPERATIONS),
            "credential_version": 0,
            "credential_id": None,
            "artifact_path": None,
            "artifact_state": "pending",
            "history": [],
        }

    def _delivery_dir(self) -> Path:
        return self._data_dir("replay") / "delivery"

    def _delivery_dir_state_exists(self, replay_code: str) -> bool:
        """同一秒内重复创建时检测磁盘是否已有同编号状态，避免凭据相互覆盖。"""
        return (self._delivery_dir() / f"{replay_code}.state.json").exists()

    def _retrieval_dir(self) -> Path:
        return self._data_dir("replay") / "retrieval_queue"

    # -- 回读与分片明细 --

    def _inspect_artifact(self, path: Path, shard_size: int = SHARD_SIZE) -> dict[str, Any]:
        """回读磁盘产物，按实际可解析记录生成分片明细并记录损坏行。"""
        if not path or not path.exists():
            return self._empty_inspection()
        records: list[dict[str, Any]] = []
        shards: list[dict[str, Any]] = []
        unreadable: list[int] = []
        record_lines: list[int] = []
        with open(path, "r", encoding="utf-8") as f:
            for lineno, line in enumerate(f, 1):
                text = line.strip()
                if not text:
                    continue
                try:
                    record = json.loads(text)
                except json.JSONDecodeError:
                    unreadable.append(lineno)
                    continue
                index = len(records) // shard_size
                if index == len(shards):
                    shards.append({
                        "shard_index": index,
                        "offset": len(records),
                        "first_line": lineno,
                        "last_line": lineno,
                        "record_count": 0,
                        "checksum": "",
                        "readable": True,
                    })
                shard = shards[index]
                shard["last_line"] = lineno
                shard["record_count"] += 1
                records.append(record)
                record_lines.append(lineno)
        for shard in shards:
            chunk = records[shard["offset"]:shard["offset"] + shard["record_count"]]
            shard["checksum"] = self._compute_checksum(
                json.dumps(chunk, ensure_ascii=False, sort_keys=True, default=str))
            shard["readable"] = shard["record_count"] > 0
        return {
            "records": records,
            "lines": record_lines,
            "shards": shards,
            "unreadable_lines": unreadable,
            "record_count": len(records),
            "checksum": self._compute_checksum(
                json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)),
            "readable": bool(records),
        }

    @staticmethod
    def _empty_inspection() -> dict[str, Any]:
        return {"records": [], "lines": [], "shards": [], "unreadable_lines": [],
                "record_count": 0, "checksum": "", "readable": False}

    @staticmethod
    def _artifact_version(inspection: dict[str, Any]) -> str:
        checksum = inspection.get("checksum") or ""
        if not checksum:
            return "unwritten"
        return f"sha256:{checksum}/{int(inspection.get('record_count', 0))}"

    # -- 来源与取回解析 --

    def _build_source(self, entry: dict[str, Any], archive_artifact: str | None) -> dict[str, Any]:
        archive_code = entry.get("归档编号", "")
        batches = self._parse_batches(entry.get("重放范围", ""), archive_code)
        batch_copies = []
        for batch_code in batches:
            path = self._data_dir("batches") / f"{batch_code}.jsonl"
            inspection = self._inspect_artifact(path, SHARD_SIZE) if path.exists() else None
            batch_copies.append({
                "batch_code": batch_code,
                "path": str(path),
                "readable": bool(inspection and inspection["readable"]),
                "record_count": int(inspection["record_count"]) if inspection else 0,
                "checksum": inspection["checksum"] if inspection else "",
            })

        source_code, target_code, window_code, contract_code = self._lineage_refs()
        return {
            "archive_code": archive_code,
            "archive_artifact": archive_artifact or "",
            "original_batches": batches,
            "batch_copies": batch_copies,
            "source_code": source_code,
            "target_code": target_code,
            "extract_window": window_code,
            "field_contract": contract_code,
        }

    def _lineage_refs(self) -> tuple[str, str, str, str]:
        source_code = target_code = window_code = contract_code = ""
        for s in store.rows("source_register"):
            if s.get("status") == "已登记":
                source_code = s.get("源端编号", "")
                break
        for t in store.rows("target_register"):
            if t.get("status") == "已登记":
                target_code = t.get("目标编号", "")
                break
        for w in store.rows("extract_window"):
            if str(w.get("窗口编号", "")).startswith("EXTR-"):
                window_code = w["窗口编号"]
                break
        for c in store.rows("field_contract"):
            if c.get("status") == "已生效":
                contract_code = c.get("契约编号", "")
                break
        return source_code, target_code, window_code, contract_code

    def _read_archive_records(self, archive_code: str) -> tuple[list[dict[str, Any]], str | None]:
        archive_dir = self._data_dir("archive")
        for ar_dir in sorted(archive_dir.iterdir()):
            if ar_dir.is_dir() and archive_code in str(ar_dir):
                for af in sorted(ar_dir.glob("*.jsonl")):
                    return self._read_jsonl(af), str(af)
        return [], None

    @staticmethod
    def _parse_batches(replay_range: str, archive_code: str) -> list[str]:
        found = re.findall(r"[A-Z]+-[A-Za-z0-9]+", replay_range or "")
        batches: list[str] = []
        for code in found:
            if code not in batches:
                batches.append(code)
        if len(batches) >= 2 and batches[0] == batches[-1]:
            batches = batches[:1]
        if batches:
            return batches
        return [archive_code] if archive_code else []

    def _resolve_batches(
        self,
        *,
        credential: dict[str, Any] | None,
        batch_code: str,
        archive_code: str,
    ) -> list[str]:
        batches: list[str] = []
        if credential:
            batches.extend((credential.get("source") or {}).get("original_batches", []))
        if batch_code and batch_code not in batches:
            batches.insert(0, batch_code)
        if not batches and archive_code:
            batches = [archive_code]
        return batches

    def _members_from_replay(
        self,
        credential: dict[str, Any],
        inspection: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
        """按凭据中登记的可读分片，从副本回读成员并标注分片与行号。"""
        shard_by_index = {int(s["shard_index"]): s for s in credential.get("shards", [])}
        lines = inspection.get("lines") or []
        members: list[dict[str, Any]] = []
        for record in inspection.get("records", []):
            position = len(members)
            shard = self._shard_for_position(shard_by_index, position)
            annotated = dict(record)
            annotated.update({
                "_shard_index": shard.get("shard_index") if shard else None,
                "_record_offset": position,
                "_shard_offset": position - int(shard.get("offset", 0)) if shard else None,
                "_line_no": lines[position] if position < len(lines) else None,
            })
            members.append(annotated)
        return members, "replay_copy", {
            "path": credential.get("artifact_path", ""),
            "credential_id": credential.get("credential_id"),
            "artifact_version": credential.get("artifact_version"),
            "shards": credential.get("shards", []),
            "unreadable_lines": credential.get("unreadable_lines", []),
        }

    @staticmethod
    def _shard_for_position(
        shard_by_index: dict[int, dict[str, Any]], position: int
    ) -> dict[str, Any] | None:
        for shard in shard_by_index.values():
            start = int(shard.get("offset", 0))
            if start <= position < start + int(shard.get("record_count", 0)):
                return shard
        return None

    def _members_from_original(
        self, batches: list[str], *, archive_code: str
    ) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
        """回读原批/归档产物作为最终可取回来源。"""
        members: list[dict[str, Any]] = []
        sources: list[dict[str, Any]] = []
        for batch_code in batches:
            path = self._data_dir("batches") / f"{batch_code}.jsonl"
            inspection = self._inspect_artifact(path, SHARD_SIZE) if path.exists() else None
            if inspection and inspection["readable"]:
                sources.append({"batch_code": batch_code, "path": str(path),
                                "record_count": inspection["record_count"],
                                "checksum": inspection["checksum"]})
                for record in inspection["records"]:
                    annotated = dict(record)
                    annotated.update({"batch_code": batch_code,
                                      "_source": "original_batch",
                                      "_record_offset": len(members)})
                    members.append(annotated)
        if members:
            return members, "original_batch", {"batches": sources}

        records, artifact = self._read_archive_records(archive_code) if archive_code else ([], None)
        if records:
            for record in records:
                annotated = dict(record)
                annotated.update({"_source": "archive_bundle",
                                  "_record_offset": len(members)})
                members.append(annotated)
            return members, "archive_bundle", {"path": artifact or "",
                                               "archive_code": archive_code,
                                               "batches": sources}
        return [], "original_batch", {"batches": sources}

    def _resolve_credential(
        self,
        *,
        batch_code: str,
        replay_code: str,
        archive_code: str,
    ) -> dict[str, Any] | None:
        """找到与原批血缘匹配的交付凭据，优先已承诺版本，其次最新版本。"""
        if replay_code:
            return self._load_credential(replay_code)
        candidates: list[dict[str, Any]] = []
        for path in sorted(self._delivery_dir().glob("*.credential.json")):
            credential = self._read_json_file(path)
            if not credential:
                continue
            source = credential.get("source") or {}
            matches_batch = bool(batch_code) and (
                batch_code == credential.get("replay_code")
                or batch_code in source.get("original_batches", [])
            )
            matches_archive = bool(archive_code) and (
                archive_code == credential.get("archive_code"))
            if matches_batch or matches_archive:
                candidates.append(credential)
        if not candidates:
            return None
        committed = [c for c in candidates if c.get("conclusion") == "已完成"]
        pool = committed or candidates
        return max(pool, key=lambda c: (int(c.get("credential_version", 0)),
                                        float(c.get("persisted_at", 0))))

    def _retrieval_result(
        self,
        *,
        status: str,
        source_kind: str,
        source_detail: dict[str, Any],
        members: list[dict[str, Any]],
        available: list[dict[str, Any]],
        credential: dict[str, Any] | None,
        explanation: str,
        request_ref: str,
    ) -> dict[str, Any]:
        cancelled_copy = None
        for item in available:
            if item["kind"] == "replay_copy" and not item["retrievable"]:
                cancelled_copy = item
                break
        return {
            "request_ref": request_ref,
            "final_status": status,
            "source": source_kind,
            "source_detail": source_detail,
            "credential_id": (credential or {}).get("credential_id"),
            "credential_version": (credential or {}).get("credential_version"),
            "delivery_state": (credential or {}).get("delivery_state"),
            "conclusion": (credential or {}).get("conclusion"),
            "members": members,
            "member_count": len(members),
            "shards_returned": sorted({
                int(m["_shard_index"]) for m in members if m.get("_shard_index") is not None
            }),
            "available_copies": available,
            "cancelled_copy": cancelled_copy,
            "explanation": explanation,
        }

    # -- store 行辅助 --

    def _project_state(self, entry: dict[str, Any], state: dict[str, Any]) -> None:
        conclusion = state.get("conclusion")
        entry["交付凭据"] = state.get("credential_id")
        entry["交付版本"] = str(state.get("credential_version") or "")
        entry["交付状态"] = state.get("delivery_state") or state.get("artifact_state") or ""
        entry["交付结论"] = conclusion or ""
        entry["交付待确认操作"] = list(state.get("pending_operations", []))
        if conclusion:
            entry["重放结果"] = conclusion

    @staticmethod
    def _state_status_is_terminal(entry: dict[str, Any], conclusion: str) -> bool:
        return entry.get("status") == conclusion

    def _conclusion_result(
        self, replay_code: str, entry: dict[str, Any], message: str
    ) -> dict[str, Any]:
        state = self._load_state(replay_code) or {}
        credential = self._load_credential(replay_code) or {}
        return {
            "replay_code": replay_code,
            "replay_id": entry.get("id"),
            "status": entry.get("status"),
            "conclusion": state.get("conclusion") or credential.get("conclusion"),
            "delivery_state": credential.get("delivery_state") or state.get("artifact_state"),
            "credential_id": credential.get("credential_id") or state.get("credential_id"),
            "credential_version": credential.get("credential_version")
            or state.get("credential_version"),
            "round": state.get("round"),
            "pending_operations": state.get("pending_operations", []),
            "source": credential.get("source", {}),
            "artifact_path": credential.get("artifact_path"),
            "artifact_version": credential.get("artifact_version"),
            "shards": credential.get("shards", []),
            "message": message,
        }

    def _recovered_result(
        self,
        entry: dict[str, Any],
        state: dict[str, Any],
        credential: dict[str, Any],
        *,
        recovered: bool,
    ) -> dict[str, Any]:
        self._project_state(entry, {**state, **credential})
        return {
            "replay_code": entry["重放编号"],
            "archive_code": credential.get("archive_code") or entry.get("归档编号"),
            "mode": credential.get("mode") or entry.get("覆盖方式"),
            "records_replayed": int(credential.get("record_count") or 0),
            "records_readable": int(credential.get("record_count") or 0),
            "artifact_path": credential.get("artifact_path"),
            "credential_id": credential.get("credential_id"),
            "credential_version": credential.get("credential_version"),
            "delivery_state": credential.get("delivery_state") or state.get("artifact_state"),
            "conclusion": credential.get("conclusion") or state.get("conclusion"),
            "shards": credential.get("shards", []),
            "unreadable_lines": credential.get("unreadable_lines", []),
            "pending_operations": state.get("pending_operations", []),
            "recovered": recovered,
            "round": state.get("round", 1),
        }

    def _code_for_id(self, replay_id: int | None) -> str | None:
        if replay_id is None:
            return None
        entry = store.find(self.MODULE, int(replay_id))
        return entry.get("重放编号") if entry else None

    def _entry_id_for_code(self, replay_code: str, replay_id: int | None) -> int | None:
        if replay_id is not None and store.find(self.MODULE, int(replay_id)):
            return int(replay_id)
        row = self._find_row_by_code(replay_code)
        return int(row["id"]) if row else None

    def _find_row_by_code(self, replay_code: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("重放编号") == replay_code:
                return row
        return None

    @staticmethod
    def _status_for_conclusion(conclusion: str | None) -> str:
        if conclusion == "已完成":
            return "已完成"
        if conclusion == "已取消":
            return "已取消"
        return "重放中"
