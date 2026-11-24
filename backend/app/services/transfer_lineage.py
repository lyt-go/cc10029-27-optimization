"""传输血缘领域服务：维护源端经抽取窗口、传输批次到目标端的交换链路血缘追溯。

本模块记录每一次数据交换的完整链路信息（源端→窗口→批次→目标端），
生成血缘链路产物文件，供下游数据追溯和质量审计使用。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class TransferLineageService(BaseModuleService):
    MODULE = "transfer_lineage"
    ENTITY = "血缘链路"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["链路编号", "源端编号", "抽取窗口", "传输批次", "目标编号", "字段契约", "血缘版本", "采集时间"]
    REQUIRED_FIELDS = ["链路编号", "源端编号", "抽取窗口"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "确认生效": "已生效", "标记停用": "已停用"}
    NEGATIVE_ACTIONS = ["标记停用"]

    def record_lineage(
        self,
        batch_code: str,
        is_balanced: bool,
        *,
        replay_code: str = "",
        credential_ref: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """记录传输批次的血缘链路：从源端经抽取窗口到目标端的完整数据交换链路。

        本方法遍历 store 中与批次关联的源端、抽取窗口、目标端记录，
        构建完整血缘链路并写入血缘产物文件，供数据追溯和质量审计。
        当批次来源是历史重放副本时，同时消费该轮的重放交付血缘凭据：
        记录当时承诺的交付版本、来源与可读分片明细，供后续取回与审计回查。
        """
        # 从 store 中查找关联的源端登记、抽取窗口和目标端记录
        source_info = {}
        target_info = {}
        window_info = {}
        contract_info = {}

        for s in store.rows("source_register"):
            if s.get("status") == "已登记":
                source_info = s
                break
        for t in store.rows("target_register"):
            if t.get("status") == "已登记":
                target_info = t
                break
        for w in store.rows("extract_window"):
            if w.get("窗口编号", "").startswith("EXTR-"):
                window_info = w
                break
        for c in store.rows("field_contract"):
            if c.get("status") == "已生效":
                contract_info = c
                break

        lineage_code = f"TRAN-{batch_code[:8]}-{int(time.time())}"
        source_code = source_info.get("源端编号", f"SOUR-{batch_code[:8]}")
        window_code = window_info.get("窗口编号", f"EXTR-{batch_code[:8]}")
        target_code = target_info.get("目标编号", f"TARG-{batch_code[:8]}")
        contract_code = contract_info.get("契约编号", f"FIEL-{batch_code[:8]}")

        # 消费重放交付血缘凭据：未显式传入时按原批血缘查找
        delivery = credential_ref or self._consume_replay_delivery(batch_code, replay_code)

        values = {
            "链路编号": lineage_code,
            "源端编号": source_code,
            "抽取窗口": window_code,
            "传输批次": batch_code,
            "目标编号": target_code,
            "字段契约": contract_code,
            "血缘版本": delivery.get("credit_version") or "v1.0",
            "采集时间": time.strftime("%Y-%m-%d", time.localtime(time.time())),
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建血缘链路失败: {missing}")

        # 写入血缘产物文件——完整链路追溯（含重放交付凭据的分片明细）
        lineage_file = self._data_dir("lineage") / f"{lineage_code}.jsonl"
        self._write_jsonl(lineage_file, [{
            "lineage_code": lineage_code,
            "source_code": source_code,
            "source_system": source_info.get("系统名称", "unknown"),
            "extract_window": window_code,
            "transfer_batch": batch_code,
            "target_code": target_code,
            "target_system": target_info.get("系统名称", "unknown"),
            "field_contract": contract_code,
            "balanced": is_balanced,
            "replay_delivery": delivery,
            "recorded_at": time.time(),
        }])

        final_entry, _ = self.run_action(entry["id"], "确认生效")

        return {
            "lineage_code": lineage_code,
            "batch_code": batch_code,
            "source": source_code,
            "target": target_code,
            "balanced": is_balanced,
            "replay_credential_id": delivery.get("credential_id"),
            "replay_delivery_state": delivery.get("delivery_state"),
            "replay_shards": len(delivery.get("shards") or []),
        }

    def _consume_replay_delivery(self, batch_code: str, replay_code: str = "") -> dict[str, Any]:
        """读取重放交付血缘凭据，返回后续取回与审计需要的版本、来源、可读分片。"""
        try:
            er_svc = self._safe_import("app.services.exchange_replay", "ExchangeReplayService")()
            credential = (er_svc.get_delivery_credential(replay_code=replay_code)
                          if replay_code else er_svc.find_credential_by_batch(batch_code))
        except Exception as e:
            return {"consumed": False, "error": str(e), "credit_version": ""}
        if not credential:
            return {"consumed": False, "credit_version": "", "shards": []}
        return {
            "consumed": True,
            "credential_id": credential.get("credential_id"),
            "credential_version": credential.get("credential_version"),
            "credit_version": f"replay-v{credential.get('credential_version')}",
            "replay_code": credential.get("replay_code"),
            "archive_code": credential.get("archive_code"),
            "delivery_state": credential.get("delivery_state"),
            "conclusion": credential.get("conclusion"),
            "artifact_version": credential.get("artifact_version"),
            "source": credential.get("source", {}),
            "shards": credential.get("shards", []),
            "unreadable_lines": credential.get("unreadable_lines", []),
            "record_count": credential.get("record_count", 0),
        }

    def query_lineage(self, batch_code: str) -> dict[str, Any]:
        """查询指定传输批次的血缘链路，追溯数据从源端到目标端的完整流转路径。"""
        lineages = []
        lineage_dir = self._data_dir("lineage")
        for lf in lineage_dir.glob(f"*{batch_code[:8]}*.jsonl"):
            lineages.extend(self._read_jsonl(lf))
        return {"batch_code": batch_code, "lineages": lineages, "count": len(lineages)}

    def get_active_lineages(self) -> list[dict[str, Any]]:
        """获取所有已生效的血缘链路列表。"""
        return [r for r in store.rows(self.MODULE) if r.get("status") == "已生效"]