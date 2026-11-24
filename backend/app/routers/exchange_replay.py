"""历史重放交付接口：重放执行、完成/取消仲裁、凭据回查、按原批血缘取回与重启恢复。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services.exchange_replay import ExchangeReplayService

router = APIRouter(prefix="/api/exchange_replay", tags=["历史重放交付"])

_svc = ExchangeReplayService()


class ExecutePayload(BaseModel):
    request_ref: str = ""
    force_new_round: bool = False


class DeliveryPayload(BaseModel):
    replay_code: str | None = None
    reason: str = ""
    request_ref: str = ""


class RetrievePayload(BaseModel):
    batch_code: str = ""
    replay_code: str = ""
    archive_code: str = ""
    request_ref: str = ""
    queue: bool = False


def _replay_id(entry_id: int | None) -> int:
    if entry_id is None:
        raise HTTPException(status_code=422, detail="缺少重放任务 id")
    return entry_id


@router.post("/{entry_id}/execute")
def execute(entry_id: int, payload: ExecutePayload | None = None) -> dict[str, Any]:
    """执行重放：写出目标副本并生成交付血缘凭据，登记两个待确认操作。"""
    body = payload or ExecutePayload()
    result = _svc.execute_replay(
        _replay_id(entry_id), request_ref=body.request_ref,
        force_new_round=body.force_new_round)
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.post("/{entry_id}/delivery/complete")
def complete(entry_id: int, payload: DeliveryPayload | None = None) -> dict[str, Any]:
    """完成请求：与取消请求相遇时按交付锁仲裁，本轮只落一个有效结论。"""
    body = payload or DeliveryPayload()
    result = _svc.complete_replay(
        entry_id, replay_code=body.replay_code, request_ref=body.request_ref)
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.post("/{entry_id}/delivery/cancel")
def cancel(entry_id: int, payload: DeliveryPayload | None = None) -> dict[str, Any]:
    """取消请求：副本已写出但未确认时可先落定取消，完成请求随后被拒。"""
    body = payload or DeliveryPayload()
    result = _svc.cancel_replay(
        entry_id, reason=body.reason, replay_code=body.replay_code,
        request_ref=body.request_ref)
    if "error" in result:
        raise HTTPException(status_code=409, detail=result["error"])
    return result


@router.get("/{entry_id}/delivery/credential")
def credential(entry_id: int, replay_code: str | None = None) -> dict[str, Any]:
    """回查重放交付血缘凭据：版本、来源、实际可读分片明细。"""
    result = _svc.get_delivery_credential(entry_id, replay_code=replay_code)
    if result is None:
        raise HTTPException(status_code=404, detail=f"重放任务 {entry_id} 无交付凭据")
    return result


@router.post("/retrieve")
def retrieve(payload: RetrievePayload) -> dict[str, Any]:
    """按原批次血缘取回当时可用的原批或重放副本，并解释取消后的可选来源。"""
    return _svc.retrieve_by_lineage(
        batch_code=payload.batch_code, replay_code=payload.replay_code,
        archive_code=payload.archive_code, request_ref=payload.request_ref,
        queue=payload.queue)


@router.post("/{entry_id}/recover")
def recover(entry_id: int, payload: DeliveryPayload | None = None) -> dict[str, Any]:
    """重启恢复：按磁盘凭据重建任务行，返回承诺过的版本与来源。"""
    body = payload or DeliveryPayload()
    code = body.replay_code
    if not code:
        entry = _svc.get_entry(entry_id)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"重放任务 {entry_id} 不存在")
        code = entry["重放编号"]
    result = _svc.recover_delivery(code)
    if "error" in result:
        raise HTTPException(status_code=404, detail=result["error"])
    return result


@router.post("/retrievals/drain")
def drain() -> dict[str, Any]:
    """继续处理排队中的取回请求（重启后调用），返回每个请求的成员与来源核对结果。"""
    results = _svc.drain_retrievals()
    return {"drained": len(results), "results": results,
            "still_queued": len(_svc.list_queued_retrievals())}
