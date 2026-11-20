"""HTTP routes for the release two-phase-commit drill."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .release_drill import (
    PAUSE_AFTER_REGISTRY_COMMITTED,
    PAUSE_AFTER_REGISTRY_PREPARED,
    PAUSE_NONE,
    ReleaseError,
    ReleaseStore,
)

logger = logging.getLogger("releasedrill.web")

# State label/order used by the UI (Chinese wording from the requirements).
STATE_LABELS = {
    "none": "未生效",
    "prepared": "已预备",
    "committed": "已提交",
}


def create_router(templates: Jinja2Templates, store: ReleaseStore) -> APIRouter:
    router = APIRouter(prefix="/release", tags=["release-2pc"])

    # Overall verdict, derived only from the two participants' live states.
    def order_overall_text(order: dict[str, Any]) -> str:
        if order["registry_state"] == "committed" and order["quota_state"] == "committed":
            return "发布成功"
        if (
            order["registry_state"] == "none"
            and order["quota_state"] == "none"
            and order["decision"] is not None
        ):
            return "已撤销"
        if order["registry_state"] == "none" and order["quota_state"] == "none":
            return "未生效"
        return "两阶段提交进行中"

    def order_overall_class(order: dict[str, Any]) -> str:
        text = order_overall_text(order)
        return {
            "发布成功": "status-recovered",
            "已撤销": "status-revoked",
            "未生效": "status-none",
            "两阶段提交进行中": "status-releasing",
        }[text]

    templates.env.globals["order_overall_text"] = order_overall_text
    templates.env.globals["order_overall_class"] = order_overall_class

    def _error(
        request: Request,
        order_no: str | None,
        exc: ReleaseError,
        *,
        status_code: int | None = None,
        form: dict[str, str] | None = None,
    ) -> HTMLResponse:
        if order_no:
            return _render(
                request,
                order_no=order_no,
                error=str(exc),
                status_code=status_code or exc.status_code,
                form=form,
            )
        return _render(
            request,
            error=str(exc),
            status_code=status_code or exc.status_code,
            form=form,
        )

    def _render(
        request: Request,
        *,
        order_no: str | None = None,
        message: str | None = None,
        error: str | None = None,
        status_code: int = 200,
        form: dict[str, str] | None = None,
    ) -> HTMLResponse:
        listing = store.list_orders()
        order = (
            store.get_order(order_no)
            if order_no and store.exists(order_no)
            else None
        )
        context: dict[str, Any] = {
            "request": request,
            "listing": listing,
            "order": order,
            "message": message,
            "error": error,
            "state_labels": STATE_LABELS,
            "form": form or {},
            "pause_choices": [
                (PAUSE_NONE, "不暂停（一次走完两阶段提交）"),
                (PAUSE_AFTER_REGISTRY_PREPARED, "暂停点 A：登记库已预备、额度库尚未预备"),
                (PAUSE_AFTER_REGISTRY_COMMITTED, "暂停点 B：登记库已提交、额度库尚未提交"),
            ],
        }
        return templates.TemplateResponse(
            request, "release.html", context, status_code=status_code
        )

    # -- pages -----------------------------------------------------------
    @router.get("", response_class=HTMLResponse)
    @router.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def release_index(request: Request) -> HTMLResponse:
        return _render(request)

    @router.get("/orders/{order_no}", response_class=HTMLResponse)
    async def order_page(request: Request, order_no: str) -> HTMLResponse:
        if not store.exists(order_no):
            return _render(
                request,
                error=f"未找到发布单 {order_no}：库中没有该单的登记、预备事务或提交决定。",
                status_code=404,
            )
        return _render(request, order_no=order_no)

    # -- form actions ----------------------------------------------------
    @router.post("/submit", response_class=HTMLResponse)
    async def submit_form(
        request: Request,
        order_no: str = Form(...),
        version: str = Form(...),
        pause: str = Form(PAUSE_NONE),
    ) -> HTMLResponse:
        try:
            view = store.submit(order_no, version, pause)
        except ReleaseError as exc:
            return _error(
                request,
                order_no.strip() or None,
                exc,
                form={"order_no": order_no, "version": version},
            )
        # PRG: reload the order page so a refresh never re-posts.
        return RedirectResponse(
            url=f"/release/orders/{view['order_no']}?msg="
            + _redirect_hint(view, "提交请求已受理"),
            status_code=303,
        )

    @router.post("/orders/{order_no}/continue", response_class=HTMLResponse)
    async def continue_form(request: Request, order_no: str) -> HTMLResponse:
        try:
            view = store.continue_order(order_no)
        except ReleaseError as exc:
            return _error(request, order_no, exc)
        return RedirectResponse(
            url=f"/release/orders/{order_no}?msg="
            + _redirect_hint(view, "继续处理完成"),
            status_code=303,
        )

    # -- JSON API --------------------------------------------------------
    @router.get("/api/orders", response_class=JSONResponse)
    async def api_list() -> JSONResponse:
        return JSONResponse({"ok": True, **store.list_orders()})

    @router.get("/api/orders/{order_no}", response_class=JSONResponse)
    async def api_order(order_no: str) -> JSONResponse:
        return JSONResponse({"ok": True, "order": store.get_order(order_no)})

    @router.post("/api/submit", response_class=JSONResponse)
    async def api_submit(
        order_no: str = Form(...),
        version: str = Form(...),
        pause: str = Form(PAUSE_NONE),
    ) -> JSONResponse:
        try:
            view = store.submit(order_no, version, pause)
        except ReleaseError as exc:
            return JSONResponse(
                {"ok": False, "error": str(exc), "kind": type(exc).__name__},
                status_code=exc.status_code,
            )
        return JSONResponse({"ok": True, "order": view})

    @router.post("/api/orders/{order_no}/continue", response_class=JSONResponse)
    async def api_continue(order_no: str) -> JSONResponse:
        try:
            view = store.continue_order(order_no)
        except ReleaseError as exc:
            return JSONResponse(
                {"ok": False, "error": str(exc), "kind": type(exc).__name__},
                status_code=exc.status_code,
            )
        return JSONResponse({"ok": True, "order": view})

    return router


def _redirect_hint(view: dict[str, Any], fallback: str) -> str:
    from urllib.parse import quote

    text = view.get("resume_message") or view.get("hint") or fallback
    return quote(text)
