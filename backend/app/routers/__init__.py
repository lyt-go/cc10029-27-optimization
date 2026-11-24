"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

历史重放另有独立交付路由（完成/取消仲裁、凭据回查、按血缘取回、重启恢复），
必须注册在通用 /api/{module} 动态路由之前，避免被抢占匹配。
"""
from __future__ import annotations

from app.routers import exchange_replay, resource

ROUTERS = [exchange_replay, resource]
