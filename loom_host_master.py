#!/usr/bin/env python3
"""
Autonomous SMIT loom host/master application with interactive dashboard.

Built on top of loom_host_v2.py and adds:
- dashboard command buttons for monitoring, configuration, and selected write actions
- compact modern UI with capabilities and live-events pages
- subcommands for service mode, self-test, and one-shot TS reads/writes

Typical use:
    python loom_host_master.py --config loom_host_master_config.example.json run --log-level INFO
    python loom_host_master.py --config loom_host_master_config.example.json selftest
    python loom_host_master.py --config loom_host_master_config.example.json status --loom-ip 169.254.4.101
    python loom_host_master.py --config loom_host_master_config.example.json speed --loom-ip 169.254.4.101
    python loom_host_master.py --config loom_host_master_config.example.json popup --loom-ip 169.254.4.101 --message "Host online"
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import ftplib
import hashlib
import html
import io
import json
import logging
import re
import signal
import socket
import sys
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import loom_host_v2 as core

# Additional commands implemented here.
CMD_PATTERN = 0x1A
CMD_BASIC_CONFIG = 0x1B
CMD_TOTAL_PICKS = 0x21
CMD_LAMP = 0x28
CMD_PRESELECTION = 0x33
CMD_REMOTE_CONTROL = 0x42
CMD_DENSITY = 0x96

DEFAULT_FTP_PORT = 21
DEFAULT_FTP_USER = "root"
DEFAULT_FTP_PASSWORD = "root"
DEFAULT_FTP_APP_DIR = "/usr/LOOM"

BEIJING_TZ = timezone(timedelta(hours=8))

def bj_now_iso() -> str:
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

def to_beijing_time_text(value: Any) -> str:
    if value in (None, ""):
        return "—"
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return str(value)
    s = str(value).strip()
    if not s:
        return "—"
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc).astimezone(BEIJING_TZ)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is not None:
            return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
        if "T" in s:
            return (dt.replace(tzinfo=timezone.utc).astimezone(BEIJING_TZ)).strftime("%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return s

RESULT_TEXT = {
    0x00: "OK",
    0x01: "OK",
    0x02: "OK",
    0x03: "OK",
    0x04: "OK",
    0x55: "OK",
    0x77: "Invalid pattern / Testana name",
    0x88: "Parameter out of range",
    0x99: "Bad data format",
    0xAA: "Communication error with loom",
    0xBB: "Motor OFF",
    0xCC: "Loom weaving / busy",
}

UNIT_PRESELECTION = {
    1: "picks",
    2: "decimetres",
    3: "yards",
    4: "pattern repeats",
    5: "metres",
}

UNIT_DENSITY = {
    0: "weft/cm",
    1: "weft/dm",
    2: "weft/inch",
}

PATTERN_FORMAT = {
    0xA5: "Flat / standard",
    0xA6: "Terry / extended",
    0x6A: "Terry / reduced",
}


WRITE_AUTH_USER = "smit"
WRITE_AUTH_PASSWORD = "2fast"
WRITE_AUTH_COOKIE = "loom_write_auth"
WRITE_AUTH_SECRET = "smit-2fast-dashboard-auth-v1"
WRITE_ACTION_PATHS = {
    "/action/pattern-send",
    "/action/lamp-write",
    "/action/preselection-write",
    "/action/preselection-reset",
    "/action/admin-declaration",
    "/action/remote-stop",
}


class InteractiveDashboardServer(core.HttpDashboardServer):
    def __init__(
        self,
        app: "MasterApp",
        registry: core.LoomRegistry,
        sink: core.EventSink,
        store: Optional[core.SQLiteStore],
        logger: logging.Logger,
    ) -> None:
        super().__init__(registry, sink, store, logger)
        self.app = app

    @staticmethod
    def _today_key() -> str:
        return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    @classmethod
    def _cookie_max_age(cls) -> int:
        now = datetime.now(BEIJING_TZ)
        tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return max(60, int((tomorrow - now).total_seconds()))

    @classmethod
    def _make_auth_cookie_value(cls) -> str:
        day = cls._today_key()
        sig = hashlib.sha256(f"{WRITE_AUTH_USER}|{day}|{WRITE_AUTH_SECRET}".encode("utf-8")).hexdigest()
        return f"{day}.{sig}"

    @classmethod
    def _auth_cookie_valid(cls, cookie_header: str) -> bool:
        if not cookie_header:
            return False
        jar = SimpleCookie()
        try:
            jar.load(cookie_header)
        except Exception:
            return False
        morsel = jar.get(WRITE_AUTH_COOKIE)
        if not morsel:
            return False
        value = morsel.value
        if "." not in value:
            return False
        day, sig = value.split(".", 1)
        expected = hashlib.sha256(f"{WRITE_AUTH_USER}|{day}|{WRITE_AUTH_SECRET}".encode("utf-8")).hexdigest()
        return day == cls._today_key() and sig == expected

    @staticmethod
    def _is_write_action(path: str) -> bool:
        return path in WRITE_ACTION_PATHS

    async def _send_response_ex(self, writer: asyncio.StreamWriter, status: int, content_type: str, body: bytes, extra_headers: Optional[List[Tuple[str, str]]] = None) -> None:
        reason = {
            200: "OK",
            401: "Unauthorized",
            403: "Forbidden",
            404: "Not Found",
            405: "Method Not Allowed",
            500: "Internal Server Error",
        }.get(status, "OK")
        headers = [
            f"HTTP/1.1 {status} {reason}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            "Connection: close",
            "Cache-Control: no-store",
        ]
        for key, value in (extra_headers or []):
            headers.append(f"{key}: {value}")
        headers.extend(["", ""])
        writer.write("\r\n".join(headers).encode("ascii") + body)
        await writer.drain()

    async def _json_ex(self, writer: asyncio.StreamWriter, data: Dict[str, Any], status: int = 200, extra_headers: Optional[List[Tuple[str, str]]] = None) -> None:
        body = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        await self._send_response_ex(writer, status, "application/json; charset=utf-8", body, extra_headers=extra_headers)

    @staticmethod
    def _logo_file_candidates() -> List[Path]:
        base = Path(__file__).resolve().parent
        names = ["LOGO_SMIT_R.JPG", "LOGO_SMIT_R.jpg", "logo_smit_r.jpg", "logo_smit_r.jpeg", "logo.jpg", "logo.jpeg", "logo.png"]
        return [base / n for n in names]

    @classmethod
    def _load_logo_asset(cls) -> Optional[Tuple[bytes, str]]:
        for path in cls._logo_file_candidates():
            if path.exists():
                suffix = path.suffix.lower()
                content_type = "image/png" if suffix == ".png" else "image/jpeg"
                return path.read_bytes(), content_type
        return None

    @staticmethod
    def _display_loom_name(name: Any) -> str:
        raw = "" if name is None else str(name)
        return "2fast-loom-01" if raw == "workshop-loom-01" else raw

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=5.0)
            header_text = head.decode("iso-8859-1", errors="replace")
            first_line = header_text.split("\r\n", 1)[0]
            method, target, _ = first_line.split(" ", 2)
            headers: Dict[str, str] = {}
            for raw_line in header_text.split("\r\n")[1:]:
                if not raw_line:
                    continue
                if ":" in raw_line:
                    k, v = raw_line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            body = b""
            if method == "POST":
                content_length = int(headers.get("content-length", "0") or "0")
                if content_length:
                    body = await asyncio.wait_for(reader.readexactly(content_length), timeout=5.0)

            parsed = urlparse(target)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if method == "GET":
                if path == "/health":
                    await self._send_response(writer, 200, "text/plain; charset=utf-8", b"OK\n")
                    return
                if path == "/api/looms":
                    payload = json.dumps(self.app.dashboard_snapshot(), indent=2, default=str).encode("utf-8")
                    await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                    return
                if path == "/api/events":
                    limit = min(max(int(qs.get("limit", ["100"])[0]), 1), 1000)
                    payload = json.dumps(self.store.recent_events(limit) if self.store else [], indent=2, default=str).encode("utf-8")
                    await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                    return
                if path == "/api/frames":
                    limit = min(max(int(qs.get("limit", ["100"])[0]), 1), 1000)
                    payload = json.dumps(self.store.recent_frames(limit) if self.store else [], indent=2, default=str).encode("utf-8")
                    await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                    return
                if path == "/fragment/dashboard-cards":
                    payload = json.dumps({
                        "ok": True,
                        "generated_at": bj_now_iso(),
                        "html": self._render_dashboard_cards(self.app.dashboard_snapshot()),
                    }, ensure_ascii=False).encode("utf-8")
                    await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                    return
                if path == "/":
                    await self.app.refresh_dashboard(force=False)
                    page = self._render_dashboard_html()
                    await self._send_response(writer, 200, "text/html; charset=utf-8", page.encode("utf-8"))
                    return
                if path == "/capabilities":
                    page = self._render_capabilities_html()
                    await self._send_response(writer, 200, "text/html; charset=utf-8", page.encode("utf-8"))
                    return
                if path == "/live":
                    page = self._render_live_html()
                    await self._send_response(writer, 200, "text/html; charset=utf-8", page.encode("utf-8"))
                    return
                if path == "/machines":
                    page = self._render_machines_html()
                    await self._send_response(writer, 200, "text/html; charset=utf-8", page.encode("utf-8"))
                    return
                if path == "/static/logo.jpg":
                    asset = self._load_logo_asset()
                    if asset is None:
                        await self._send_response_ex(writer, 404, "text/plain; charset=utf-8", b"Logo not found")
                    else:
                        data, content_type = asset
                        await self._send_response_ex(writer, 200, content_type, data)
                    return
                page = self._render_dashboard_html()
                await self._send_response(writer, 200, "text/html; charset=utf-8", page.encode("utf-8"))
                return

            if method == "POST":
                await self._handle_post(path, body, writer, headers)
                return

            await self._send_response(writer, 405, "text/plain; charset=utf-8", b"Method Not Allowed")
        except asyncio.IncompleteReadError:
            pass
        except (asyncio.TimeoutError, asyncio.CancelledError, ConnectionResetError, BrokenPipeError):
            # Browser preconnects, aborted refreshes, extensions, or partial requests
            # should not be treated as dashboard server errors.
            pass
        except Exception as exc:
            self.log.exception("HTTP dashboard request failed")
            try:
                payload = json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False).encode("utf-8")
                await self._send_response(writer, 500, "application/json; charset=utf-8", payload)
            except Exception:
                pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_post(self, path: str, body: bytes, writer: asyncio.StreamWriter, headers: Dict[str, str]) -> None:
        form = parse_qs(body.decode("utf-8", errors="replace"), keep_blank_values=True)
        if path == "/auth/login":
            username = (form.get("username", [""])[0] or "").strip()
            password = (form.get("password", [""])[0] or "")
            if username == WRITE_AUTH_USER and password == WRITE_AUTH_PASSWORD:
                cookie_value = self._make_auth_cookie_value()
                max_age = self._cookie_max_age()
                cookie = f"{WRITE_AUTH_COOKIE}={cookie_value}; Max-Age={max_age}; Path=/; SameSite=Lax"
                await self._json_ex(writer, {"ok": True, "message": "Write access enabled for today"}, extra_headers=[("Set-Cookie", cookie)])
            else:
                await self._json_ex(writer, {"ok": False, "error": "Invalid username or password"}, status=403)
            return

        if self._is_write_action(path) and (not self._auth_cookie_valid(headers.get("cookie", ""))):
            await self._json_ex(writer, {"ok": False, "error": "Write authentication required"}, status=403)
            return

        loom_ip = (form.get("loom_ip", [""])[0] or "").strip()
        message = (form.get("message", [""])[0] or "").strip()
        pattern_name = (form.get("pattern_name", [""])[0] or "").strip()
        raw_calls = (form.get("calls_mask", [""])[0] or "").strip()
        code_str = (form.get("code_str", [""])[0] or "").strip()
        template = (form.get("template", [""])[0] or "").strip()

        try:
            if path == "/action/read-all":
                await self._json(writer, {"ok": True, "result": await self.app.query_all_status(self._require_loom(loom_ip), force=True)})
                return
            if path == "/action/error-logs":
                await self._json(writer, {"ok": True, "result": await self.app.query_error_logs(self._require_loom(loom_ip))})
                return
            if path == "/action/status":
                await self._json(writer, {"ok": True, "result": await self.app.query_status(self._require_loom(loom_ip))})
                return
            if path == "/action/full-status":
                await self._json(writer, {"ok": True, "result": await self.app.query_full_status(self._require_loom(loom_ip))})
                return
            if path == "/action/speed":
                await self._json(writer, {"ok": True, "result": await self.app.query_speed(self._require_loom(loom_ip))})
                return
            if path == "/action/basic-config":
                await self._json(writer, {"ok": True, "result": await self.app.query_basic_config(self._require_loom(loom_ip))})
                return
            if path == "/action/total-picks":
                await self._json(writer, {"ok": True, "result": await self.app.query_total_picks(self._require_loom(loom_ip))})
                return
            if path == "/action/density":
                await self._json(writer, {"ok": True, "result": await self.app.query_density(self._require_loom(loom_ip))})
                return
            if path == "/action/pattern-current":
                await self._json(writer, {"ok": True, "result": await self.app.query_pattern_current(self._require_loom(loom_ip))})
                return
            if path == "/action/pattern-info":
                await self._json(writer, {"ok": True, "result": await self.app.query_pattern_info(self._require_loom(loom_ip), self._require_text(pattern_name, "pattern_name"))})
                return
            if path == "/action/pattern-send":
                await self._json(writer, {"ok": True, "result": await self.app.send_pattern(self._require_loom(loom_ip), self._require_text(pattern_name, "pattern_name"))})
                return
            if path == "/action/lamp-read":
                await self._json(writer, {"ok": True, "result": await self.app.query_lamp_tree(self._require_loom(loom_ip))})
                return
            if path == "/action/lamp-write":
                await self._json(writer, {"ok": True, "result": await self.app.write_lamp_tree(self._require_loom(loom_ip), int(raw_calls, 0))})
                return
            if path == "/action/preselection-read":
                await self._json(writer, {"ok": True, "result": await self.app.query_preselection(self._require_loom(loom_ip))})
                return
            if path == "/action/preselection-write":
                payload = {
                    "enabled": self._as_bool(form, "enabled"),
                    "stop_on_reach": self._as_bool(form, "stop_on_reach"),
                    "confirm_before_restart": self._as_bool(form, "confirm_before_restart"),
                    "delayed_stop": self._as_bool(form, "delayed_stop"),
                    "manual_reset": self._as_bool(form, "manual_reset"),
                    "unit": int((form.get("unit", [""])[0] or "0").strip()),
                    "preselection_value": int((form.get("preselection_value", [""])[0] or "0").strip()),
                    "counter_value": int((form.get("counter_value", ["0"])[0] or "0").strip()),
                }
                await self._json(writer, {"ok": True, "result": await self.app.write_preselection(self._require_loom(loom_ip), payload)})
                return
            if path == "/action/preselection-reset":
                await self._json(writer, {"ok": True, "result": await self.app.reset_preselection(self._require_loom(loom_ip))})
                return
            if path == "/action/admin-declaration":
                result = self.app.configure_admin_declaration(
                    self._require_loom(loom_ip),
                    code_str=(code_str or "565"),
                    template=(template or "Employee ID: _____ Production Plan ID: _____ Yarn Package Number: _____"),
                )
                await self._json(writer, {"ok": True, "result": result})
                return
            if path == "/action/machine-add":
                result = self.app.upsert_loom_config(
                    old_ip=None,
                    name=(form.get("name", [""])[0] or "").strip(),
                    new_ip=self._require_text((form.get("new_ip", [""])[0] or "").strip(), "new_ip"),
                    ts_port=int((form.get("ts_port", [str(core.DEFAULT_TS_PORT)])[0] or str(core.DEFAULT_TS_PORT)).strip()),
                    supports_qt5_full_status=self._as_bool(form, "supports_qt5_full_status"),
                    enabled=self._as_bool(form, "enabled", default=True),
                )
                await self._json(writer, {"ok": True, "result": result})
                return
            if path == "/action/machine-save":
                result = self.app.upsert_loom_config(
                    old_ip=(form.get("old_ip", [""])[0] or "").strip() or None,
                    name=(form.get("name", [""])[0] or "").strip(),
                    new_ip=self._require_text((form.get("new_ip", [""])[0] or "").strip(), "new_ip"),
                    ts_port=int((form.get("ts_port", [str(core.DEFAULT_TS_PORT)])[0] or str(core.DEFAULT_TS_PORT)).strip()),
                    supports_qt5_full_status=self._as_bool(form, "supports_qt5_full_status"),
                    enabled=self._as_bool(form, "enabled", default=True),
                )
                await self._json(writer, {"ok": True, "result": result})
                return
            if path == "/action/machine-delete":
                result = self.app.delete_loom_config(self._require_loom(loom_ip))
                await self._json(writer, {"ok": True, "result": result})
                return
            if path == "/action/remote-stop":
                await self._json(writer, {"ok": True, "result": await self.app.remote_stop(self._require_loom(loom_ip))})
                return
            if path == "/action/refresh-all":
                results = []
                for loom in self.app.registry.all_known_looms():
                    if not self.app.is_loom_enabled(loom.ip):
                        continue
                    try:
                        payload = await self.app.query_all_status(loom.ip, force=True)
                        results.append({"loom_ip": loom.ip, "ok": True, "result": payload})
                    except Exception as exc:
                        results.append({"loom_ip": loom.ip, "ok": False, "error": f"{type(exc).__name__}: {exc}"})
                await self._json(writer, {"ok": True, "results": results})
                return
            await self._send_response(writer, 404, "application/json; charset=utf-8", json.dumps({"ok": False, "error": "Not Found"}).encode("utf-8"))
        except Exception as exc:
            await self._json(writer, {"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)

    @staticmethod
    def _require_loom(loom_ip: str) -> str:
        if not loom_ip:
            raise ValueError("loom_ip is required")
        return loom_ip

    @staticmethod
    def _require_text(value: str, name: str) -> str:
        if not value:
            raise ValueError(f"{name} is required")
        return value

    @staticmethod
    def _as_bool(form: Dict[str, List[str]], key: str, default: bool = False) -> bool:
        if key not in form:
            return default
        value = (form.get(key, [""])[0] or "").strip().lower()
        return value in {"1", "true", "yes", "y", "on"}

    async def _json(self, writer: asyncio.StreamWriter, data: Dict[str, Any], status: int = 200) -> None:
        await self._send_response(writer, status, "application/json; charset=utf-8", json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8"))

    @staticmethod
    def _escape(value: Any) -> str:
        return html.escape("" if value is None else str(value))

    @staticmethod
    def _fmt_age(ts: Any) -> str:
        if not ts:
            return "—"
        try:
            delta = max(0.0, core.time.time() - float(ts))
            if delta < 2:
                return "just now"
            if delta < 60:
                return f"{delta:.0f}s"
            if delta < 3600:
                return f"{delta/60:.1f}m"
            return f"{delta/3600:.1f}h"
        except Exception:
            return "—"

    @staticmethod
    def _fmt_efficiency(value: Any) -> str:
        if value in (None, ""):
            return "—"
        try:
            return f"{float(value) / 100:.2f}%"
        except Exception:
            return str(value)

    def _nav(self, current: str) -> str:
        links = [
            ("/", "Dashboard"),
            ("/machines", "Machine IPs"),
        ]
        items = []
        for href, label in links:
            active = " active" if href == current else ""
            items.append(f'<a class="nav-link{active}" href="{href}">{self._escape(label)}</a>')
        return "".join(items)

    def _brand_logo_svg(self) -> str:
        return """<svg viewBox="0 0 260 90" width="150" height="44" role="img" aria-label="SMIT logo" xmlns="http://www.w3.org/2000/svg" style="display:block;width:150px;height:44px;overflow:visible">
  <text x="0" y="64" fill="#1677C8" font-size="62" font-weight="800" font-family="Arial, Helvetica, sans-serif" letter-spacing="-1">SMIT</text>
</svg>"""

    def _base_page(self, title: str, current: str, body_html: str, scripts: str = "") -> str:
        return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>{self._escape(title)}</title>
  <style>
    :root {{
      --bg:#f4f7fb;
      --panel:#ffffff;
      --panel-2:#fbfcff;
      --line:#e5ebf3;
      --text:#122033;
      --muted:#6b7a90;
      --blue:#2f6bff;
      --purple:#7c4dff;
      --orange:#ff8f1f;
      --green:#16a34a;
      --yellow:#f6c445;
      --red:#ef4444;
      --pink:#ff4d8d;
      --chip:#eef3ff;
      --shadow:0 8px 24px rgba(26, 46, 80, .08);
      --radius:18px;
    }}
    * {{ box-sizing:border-box; }}
    html,body {{ margin:0; padding:0; font-family: Inter, SF Pro Display, Segoe UI, Arial, sans-serif; color:var(--text); background:linear-gradient(180deg,#eef4ff 0%,#f8fbff 35%,#f5f7fb 100%); }}
    .wrap {{ max-width: 1580px; margin: 0 auto; padding: 18px; }}
    .topbar {{ display:flex; gap:16px; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; margin-bottom:16px; }}
    .title-block h1 {{ margin:0; font-size: 28px; letter-spacing:-0.03em; }}
    .brand {{ display:flex; align-items:center; gap:14px; }}
    .brand-logo {{ width:auto; height:58px; object-fit:contain; filter: drop-shadow(0 4px 10px rgba(20,40,80,.08)); }}
    .brand-copy {{ display:flex; flex-direction:column; gap:2px; }}
    .muted {{ color:var(--muted); font-size:13px; margin-top:4px; }}
    .nav {{ display:flex; flex-wrap:wrap; gap:8px; }}
    .nav-link {{ text-decoration:none; font-size:13px; padding:9px 12px; border-radius:999px; background:rgba(255,255,255,.8); color:var(--text); border:1px solid var(--line); box-shadow:var(--shadow); }}
    .nav-link.active {{ background:linear-gradient(135deg,var(--blue),var(--purple)); color:white; border-color:transparent; }}
    .toolbar {{ display:flex; flex-wrap:wrap; gap:10px; margin:12px 0 18px; }}
    .button {{ appearance:none; border:none; border-radius:12px; padding:10px 13px; font-weight:700; color:white; background:linear-gradient(135deg,var(--blue),var(--purple)); cursor:pointer; box-shadow:0 10px 20px rgba(47,107,255,.22); font-size:13px; }}
    .button:hover {{ filter:brightness(1.04); }}
    .button.alt {{ background:linear-gradient(135deg,#22c55e,#16a34a); box-shadow:0 10px 20px rgba(34,197,94,.18); }}
    .button.warn {{ background:linear-gradient(135deg,#ff9f1a,#ff6f00); box-shadow:0 10px 20px rgba(255,143,31,.20); }}
    .button.danger {{ background:linear-gradient(135deg,#ff4d6d,#ef4444); box-shadow:0 10px 20px rgba(239,68,68,.22); }}
    .button.ghost {{ background:#fff; color:var(--text); border:1px solid var(--line); box-shadow:none; }}
    .cards {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(380px,1fr)); gap:16px; }}
    .loom-card {{ background:var(--panel); border:1px solid var(--line); border-radius:24px; padding:16px; box-shadow:var(--shadow); }}
    .card-top {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom:12px; }}
    .card-top h2 {{ margin:0; font-size:24px; letter-spacing:-0.03em; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:3px; }}
    .pill {{ border-radius:999px; padding:8px 12px; font-size:12px; font-weight:800; white-space:nowrap; }}
    .pill.ok {{ background:#ddfbe8; color:#0f7a3b; }}
    .pill.warn {{ background:#fff3cf; color:#9a5c00; }}
    .pill.stop {{ background:#ffe1e6; color:#b4233a; }}
    .banner {{ border-radius:16px; padding:14px 16px; font-size:15px; font-weight:800; margin-bottom:12px; }}
    .banner.running {{ background:linear-gradient(135deg,#dff8e7,#c4f0d1); color:#11653b; }}
    .banner.stopped {{ background:linear-gradient(135deg,#ffe4e8,#ffd7dd); color:#a61d33; }}
    .actions {{ display:grid; gap:12px; margin-bottom:14px; }}
    .action-sections {{ display:grid; grid-template-columns:1fr; gap:10px; margin-bottom:12px; }}
    .action-row {{ display:grid; grid-template-columns:110px repeat(3, max-content); gap:12px; align-items:center; }}
    .action-row .button {{ min-width:170px; justify-self:start; }}
    .action-label {{ min-width:96px; font-size:11px; font-weight:800; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; }}
    .metrics-grid {{ display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:10px; margin-bottom:12px; }}
    .metric-tile {{ background:var(--panel-2); border:1px solid var(--line); border-radius:16px; padding:10px 12px; min-height:86px; }}
    .metric-label {{ font-size:11px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.07em; margin-bottom:6px; }}
    .metric-value {{ font-size:24px; font-weight:900; letter-spacing:-0.03em; line-height:1.05; }}
    .metric-sub {{ font-size:12px; color:var(--muted); margin-top:4px; }}
    .info-grid {{ display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:10px; }}
    .info-card {{ background:#fff; border:1px solid var(--line); border-radius:14px; padding:10px 11px; min-height:68px; }}
    .info-label {{ font-size:10px; font-weight:800; color:var(--muted); text-transform:uppercase; letter-spacing:.1em; margin-bottom:5px; }}
    .info-value {{ font-size:14px; line-height:1.3; word-break:break-word; }}
    .chipline {{ display:flex; flex-wrap:wrap; gap:6px; margin-top:8px; }}
    .chip {{ padding:6px 10px; border-radius:999px; background:var(--chip); border:1px solid #dbe4ff; color:#27407c; font-size:12px; }}
    .section {{ background:var(--panel); border:1px solid var(--line); border-radius:24px; padding:16px; box-shadow:var(--shadow); margin-top:18px; }}
    .section h2 {{ margin:0 0 12px 0; font-size:18px; }}
    table {{ width:100%; border-collapse:collapse; background:#fff; overflow:hidden; border-radius:16px; border:1px solid var(--line); }}
    th, td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; font-size:13px; }}
    th {{ background:#f5f8ff; color:#50617b; font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    tr:last-child td {{ border-bottom:none; }}
    code, pre {{ font-family: ui-monospace, SFMono-Regular, Consolas, monospace; }}
    pre {{ white-space:pre-wrap; word-break:break-word; margin:0; font-size:12px; background:#f8fafc; border-radius:12px; padding:10px; border:1px solid var(--line); }}
    .toast {{ position:fixed; right:18px; bottom:18px; border-radius:14px; padding:12px 14px; color:white; max-width:420px; z-index:9999; box-shadow:0 16px 32px rgba(20, 20, 30, .28); opacity:0; transform:translateY(10px); transition:opacity .15s ease, transform .15s ease; }}
    .toast.show {{ opacity:1; transform:translateY(0); }}
    .toast.ok {{ background:linear-gradient(135deg,#16a34a,#22c55e); }}
    .toast.error {{ background:linear-gradient(135deg,#ef4444,#d73c61); }}
    .modal-backdrop {{ position:fixed; inset:0; background:rgba(15,23,42,.38); backdrop-filter: blur(8px); display:none; align-items:center; justify-content:center; z-index:12000; padding:18px; }}
    .modal-backdrop.show {{ display:flex; }}
    .modal-card {{ width:min(520px, 100%); background:rgba(255,255,255,.96); border:1px solid var(--line); border-radius:22px; box-shadow:0 24px 64px rgba(15,23,42,.24); overflow:hidden; }}
    .modal-head {{ padding:16px 18px 10px; border-bottom:1px solid var(--line); }}
    .modal-title {{ margin:0; font-size:18px; letter-spacing:-.02em; }}
    .modal-sub {{ margin-top:4px; color:var(--muted); font-size:13px; }}
    .modal-body {{ padding:16px 18px; display:grid; gap:12px; }}
    .modal-grid {{ display:grid; gap:12px; }}
    .modal-field {{ display:grid; gap:6px; }}
    .modal-field label {{ font-size:12px; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; }}
    .modal-field input, .modal-field textarea, .modal-field select {{ width:100%; border:1px solid var(--line); border-radius:12px; padding:11px 12px; font:inherit; background:#fff; color:var(--text); }}
    .modal-field textarea {{ min-height:96px; resize:vertical; }}
    .modal-checkbox {{ display:flex; gap:10px; align-items:center; padding:10px 0; }}
    .modal-checkbox input {{ width:18px; height:18px; }}
    .modal-actions {{ display:flex; justify-content:flex-end; gap:10px; padding:14px 18px 18px; border-top:1px solid var(--line); }}
    .modal-note {{ font-size:13px; color:var(--muted); line-height:1.45; }}
    @media (max-width: 1180px) {{ .metrics-grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }} .info-grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }} .admin-grid {{ grid-template-columns: repeat(3, minmax(0,1fr)); }} .action-row {{ grid-template-columns:110px repeat(2, max-content); }} }}
    @media (max-width: 860px) {{ .wrap {{ padding:12px; }} .cards {{ grid-template-columns:1fr; }} .metrics-grid {{ grid-template-columns:1fr 1fr; }} .info-grid {{ grid-template-columns:1fr; }} .admin-grid {{ grid-template-columns:1fr 1fr; }} .action-row {{ grid-template-columns:1fr; gap:10px; }} .action-row .button {{ width:100%; min-width:unset; }} .action-label {{ min-width:unset; width:100%; }} }}
    @media (max-width: 560px) {{ .admin-grid {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="topbar">
      <div class="title-block">
        <div class="brand">
          <div class="brand-logo-wrap">{self._brand_logo_svg()}</div>
          <div class="brand-copy">
            <h1>Loom Real-Time Monitoring Panel</h1>
            <div class="muted">Auto-refresh every 8 seconds · Generated at {self._escape(bj_now_iso())}</div>
          </div>
        </div>
      </div>
      <nav class="nav">{self._nav(current)}</nav>
    </div>
    {body_html}
  </div>
  <div id="toast" class="toast"></div>
  <div id="appModal" class="modal-backdrop" aria-hidden="true">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="modalTitle">
      <div class="modal-head">
        <h3 id="modalTitle" class="modal-title">Action</h3>
        <div id="modalSub" class="modal-sub"></div>
      </div>
      <form id="modalForm">
        <div id="modalBody" class="modal-body"></div>
        <div class="modal-actions">
          <button type="button" id="modalCancel" class="button ghost">Cancel</button>
          <button type="submit" id="modalOk" class="button">OK</button>
        </div>
      </form>
    </div>
  </div>
  <script>
    const WRITE_AUTH_DAY_KEY = 'loomWriteAuthDay';
    const modalEl = document.getElementById('appModal');
    const modalTitleEl = document.getElementById('modalTitle');
    const modalSubEl = document.getElementById('modalSub');
    const modalBodyEl = document.getElementById('modalBody');
    const modalFormEl = document.getElementById('modalForm');
    const modalCancelEl = document.getElementById('modalCancel');
    const modalOkEl = document.getElementById('modalOk');
    let modalResolve = null;

    function localDayKey() {{
      const d = new Date();
      const y = d.getFullYear();
      const m = String(d.getMonth() + 1).padStart(2, '0');
      const day = String(d.getDate()).padStart(2, '0');
      return `${{y}}-${{m}}-${{day}}`;
    }}
    function escHtml(value) {{
      return String(value ?? '').replace(/[&<>"']/g, (m) => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[m]));
    }}
    function fieldHtml(field) {{
      const name = escHtml(field.name || 'field');
      const label = escHtml(field.label || field.name || 'Field');
      const value = field.value ?? '';
      if (field.type === 'checkbox') {{
        const checked = value ? 'checked' : '';
        return `<div class="modal-checkbox"><input data-field-name="${{name}}" type="checkbox" ${{checked}}><label>${{label}}</label></div>`;
      }}
      if (field.type === 'textarea') {{
        return `<div class="modal-field"><label>${{label}}</label><textarea data-field-name="${{name}}" placeholder="${{escHtml(field.placeholder || '')}}">${{escHtml(value)}}</textarea></div>`;
      }}
      const type = escHtml(field.type || 'text');
      const placeholder = escHtml(field.placeholder || '');
      const autocomplete = escHtml(field.autocomplete || 'off');
      return `<div class="modal-field"><label>${{label}}</label><input data-field-name="${{name}}" type="${{type}}" value="${{escHtml(value)}}" placeholder="${{placeholder}}" autocomplete="${{autocomplete}}"></div>`;
    }}
    function openModal(options = {{}}) {{
      return new Promise((resolve) => {{
        modalResolve = resolve;
        modalTitleEl.textContent = options.title || 'Action';
        modalSubEl.textContent = options.subtitle || '';
        modalOkEl.textContent = options.okText || 'OK';
        modalCancelEl.textContent = options.cancelText || 'Cancel';
        modalOkEl.className = `button${{options.okClass ? ' ' + options.okClass : ''}}`;
        const note = options.message ? `<div class="modal-note">${{escHtml(options.message)}}</div>` : '';
        const fields = (options.fields || []).map(fieldHtml).join('');
        modalBodyEl.innerHTML = `${{note}}<div class="modal-grid">${{fields}}</div>`;
        modalEl.classList.add('show');
        modalEl.setAttribute('aria-hidden', 'false');
        const firstInput = modalBodyEl.querySelector('input, textarea, select');
        if (firstInput) setTimeout(() => firstInput.focus(), 20);
      }});
    }}
    function closeModal(result) {{
      modalEl.classList.remove('show');
      modalEl.setAttribute('aria-hidden', 'true');
      const resolver = modalResolve;
      modalResolve = null;
      if (resolver) resolver(result);
    }}
    modalCancelEl.addEventListener('click', () => closeModal(null));
    modalEl.addEventListener('click', (event) => {{ if (event.target === modalEl) closeModal(null); }});
    modalFormEl.addEventListener('submit', (event) => {{
      event.preventDefault();
      const data = {{}};
      modalBodyEl.querySelectorAll('[data-field-name]').forEach((el) => {{
        const name = el.getAttribute('data-field-name');
        if (!name) return;
        data[name] = el.type === 'checkbox' ? (el.checked ? '1' : '0') : el.value;
      }});
      closeModal(data);
    }});

    async function postForm(path, data, options = {{}}) {{
      const body = new URLSearchParams(data);
      const res = await fetch(path, {{ method:'POST', headers:{{ 'Content-Type':'application/x-www-form-urlencoded;charset=UTF-8' }}, body, credentials:'same-origin' }});
      const payload = await res.json().catch(() => ({{ ok:false, error:'Invalid JSON reply' }}));
      if ((!res.ok || !payload.ok) && res.status === 403 && options.requireWriteAuth) {{
        localStorage.removeItem(WRITE_AUTH_DAY_KEY);
      }}
      if (!res.ok || !payload.ok) throw new Error((payload && payload.error) || `HTTP ${{res.status}}`);
      return payload;
    }}
    async function ensureWriteAuth() {{
      if (localStorage.getItem(WRITE_AUTH_DAY_KEY) === localDayKey()) return;
      const data = await openModal({{
        title: 'Write Access Login',
        subtitle: 'Required once per day for protected write operations.',
        okText: 'Unlock write access',
        fields: [
          {{ name: 'username', label: 'Username', value: 'smit', autocomplete: 'username' }},
          {{ name: 'password', label: 'Password', type: 'password', value: '', autocomplete: 'current-password' }},
        ]
      }});
      if (!data) throw new Error('Write login cancelled');
      await postForm('/auth/login', {{ username: data.username || '', password: data.password || '' }}, {{ requireWriteAuth:false }});
      localStorage.setItem(WRITE_AUTH_DAY_KEY, localDayKey());
      showToast('ok', 'Write access enabled for today');
    }}
    async function guardedPostForm(path, data) {{
      await ensureWriteAuth();
      return await postForm(path, data, {{ requireWriteAuth:true }});
    }}
    async function runProtectedSimple(action, loomIp) {{
      try {{
        await guardedPostForm(`/action/${{action}}`, {{ loom_ip: loomIp }});
        showToast('ok', `${{action}} completed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `${{action}} failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    function showToast(kind, message) {{
      const toast = document.getElementById('toast');
      toast.className = `toast show ${{kind}}`;
      toast.textContent = message;
      clearTimeout(window.__toastTimer);
      window.__toastTimer = setTimeout(() => {{ toast.className = 'toast'; }}, 3200);
    }}
    async function runSimple(action, loomIp) {{
      try {{
        await postForm(`/action/${{action}}`, {{ loom_ip: loomIp }});
        showToast('ok', `${{action}} completed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `${{action}} failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function runReadAll(loomIp) {{
      try {{
        await postForm('/action/read-all', {{ loom_ip: loomIp }});
        showToast('ok', `All status refreshed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Read all failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function promptPatternInfo(loomIp) {{
      const data = await openModal({{
        title: 'Pattern Info',
        subtitle: loomIp,
        okText: 'Read pattern info',
        fields: [{{ name: 'pattern_name', label: 'Pattern name', value: '' }}]
      }});
      if (!data) return;
      try {{
        const res = await postForm('/action/pattern-info', {{ loom_ip: loomIp, pattern_name: data.pattern_name || '' }});
        showToast('ok', `Pattern info read for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Pattern info failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function promptPatternSend(loomIp) {{
      const data = await openModal({{
        title: 'Send Pattern',
        subtitle: loomIp,
        okText: 'Send pattern',
        fields: [{{ name: 'pattern_name', label: 'Pattern name', value: '' }}]
      }});
      if (!data) return;
      try {{
        await guardedPostForm('/action/pattern-send', {{ loom_ip: loomIp, pattern_name: data.pattern_name || '' }});
        showToast('ok', `Pattern sent to ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Pattern send failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function promptLampWrite(loomIp) {{
      const data = await openModal({{
        title: 'Lamp Write',
        subtitle: loomIp,
        okText: 'Write lamp mask',
        fields: [{{ name: 'calls_mask', label: 'Calls mask (hex or int)', value: '0x0' }}]
      }});
      if (!data) return;
      try {{
        await guardedPostForm('/action/lamp-write', {{ loom_ip: loomIp, calls_mask: data.calls_mask || '0' }});
        showToast('ok', `Lamp write completed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Lamp write failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function promptPreselectionWrite(loomIp) {{
      const data = await openModal({{
        title: 'Preselection Write',
        subtitle: loomIp,
        okText: 'Write preselection',
        fields: [
          {{ name: 'enabled', label: 'Enabled', type: 'checkbox', value: true }},
          {{ name: 'stop_on_reach', label: 'Stop on reach', type: 'checkbox', value: true }},
          {{ name: 'confirm_before_restart', label: 'Confirm before restart', type: 'checkbox', value: false }},
          {{ name: 'delayed_stop', label: 'Delayed stop', type: 'checkbox', value: false }},
          {{ name: 'manual_reset', label: 'Manual reset', type: 'checkbox', value: false }},
          {{ name: 'unit', label: 'Unit', value: '1' }},
          {{ name: 'preselection_value', label: 'Preselection value', value: '0' }},
          {{ name: 'counter_value', label: 'Counter value', value: '0' }},
        ]
      }});
      if (!data) return;
      try {{
        await guardedPostForm('/action/preselection-write', {{ loom_ip: loomIp, ...data }});
        showToast('ok', `Preselection write completed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Preselection write failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function promptAdminDeclaration(loomIp) {{
      const data = await openModal({{
        title: 'Administrative Declaration',
        subtitle: loomIp,
        okText: 'Save declaration',
        fields: [
          {{ name: 'code_str', label: 'Declaration code', value: '565' }},
          {{ name: 'template', label: 'Administrative template', type: 'textarea', value: 'Employee ID: _____ Production Plan ID: _____ Yarn Package Number: _____' }},
        ]
      }});
      if (!data) return;
      try {{
        await guardedPostForm('/action/admin-declaration', {{ loom_ip: loomIp, code_str: data.code_str || '565', template: data.template || '' }});
        showToast('ok', `Administrative declaration armed for ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Administrative declaration failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    async function remoteStop(loomIp) {{
      try {{
        await guardedPostForm('/action/remote-stop', {{ loom_ip: loomIp }});
        showToast('ok', `Remote stop sent to ${{loomIp}}`);
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Remote stop failed for ${{loomIp}}: ${{err.message || err}}`);
      }}
    }}
    let liveRefreshTimer = null;
    let liveRefreshBusy = false;

    async function liveRefreshDashboard() {{
      if (liveRefreshBusy) return;
      const cardsRoot = document.getElementById('cards-root');
      if (!cardsRoot) return;
      liveRefreshBusy = true;
      try {{
        const res = await fetch('/fragment/dashboard-cards', {{ credentials:'same-origin', cache:'no-store' }});
        const payload = await res.json();
        if (res.ok && payload && payload.ok) {{
          cardsRoot.innerHTML = payload.html || '';
          const gen = document.getElementById('generated-at');
          if (gen && payload.generated_at) gen.textContent = payload.generated_at;
        }}
      }} catch (err) {{
        console.warn('Live dashboard refresh failed', err);
      }} finally {{
        liveRefreshBusy = false;
      }}
    }}

    function startLiveDashboardRefresh() {{
      if (window.location.pathname !== '/') return;
      if (liveRefreshTimer) clearInterval(liveRefreshTimer);
      liveRefreshTimer = setInterval(liveRefreshDashboard, 2000);
    }}

    async function refreshAll() {{
      try {{
        await postForm('/action/refresh-all', {{}});
        showToast('ok', 'All looms refreshed');
        setTimeout(() => liveRefreshDashboard(), 250);
      }} catch (err) {{
        showToast('error', `Refresh failed: ${{err.message || err}}`);
      }}
    }}
    function readAllStatus(loomIp) {{ return runReadAll(loomIp); }}
    function patternInfo(loomIp) {{ return promptPatternInfo(loomIp); }}

    startLiveDashboardRefresh();
    {scripts}
  </script>
</body>
</html>"""

    @staticmethod
    def _latest_log_row(logs: Dict[str, Any]) -> Optional[List[str]]:
        if not isinstance(logs, dict):
            return None
        err_rows = (((logs.get("errors") or {}).get("rows")) or [])
        if err_rows:
            return err_rows[-1]
        evt_rows = (((logs.get("events") or {}).get("rows")) or [])
        if evt_rows:
            return evt_rows[-1]
        return None

    @classmethod
    def _latest_log_reason(cls, logs: Dict[str, Any]) -> Optional[str]:
        row = cls._latest_log_row(logs)
        if not row:
            return None
        parts = []
        for idx in (3, 4, 5, 6, 7):
            if idx < len(row):
                value = str(row[idx]).strip()
                if value and value not in {'-', '—'}:
                    parts.append(value)
        if not parts:
            return None
        return ' · '.join(parts[:3])

    def _latest_admin_declaration_event(self, ip: str) -> Optional[Dict[str, Any]]:
        latest = getattr(self.app, 'last_admin_completions', {}).get(ip)
        if latest:
            return latest
        if self.store:
            try:
                for ev in self.store.recent_events(300):
                    if ev.get("peer_ip") == ip and ev.get("event_type") == "declaration_completion":
                        return ev
            except Exception:
                pass
            raw = self._latest_admin_declaration_from_frames(ip)
            if raw:
                return raw
        return None

    def _latest_admin_declaration_from_frames(self, ip: str) -> Optional[Dict[str, Any]]:
        if not self.store:
            return None
        try:
            for fr in self.store.recent_frames(500):
                if fr.get('peer_ip') != ip or fr.get('direction') != 'rx' or int(fr.get('cmd', -1)) != int(core.CMD_DECLARATION):
                    continue
                hexstr = (fr.get('hex') or '').replace(' ', '')
                if not hexstr:
                    continue
                try:
                    raw = bytes.fromhex(hexstr)
                except Exception:
                    continue
                if len(raw) < 2:
                    continue
                payload = raw[2:2+raw[1]]
                if len(payload) <= 3:
                    continue
                try:
                    text = payload.decode('ascii', errors='ignore').rstrip('\x00')
                except Exception:
                    continue
                code = None
                completed = text
                if len(text) >= 3 and text[:3].isdigit():
                    code = text[:3]
                    completed = text[3:].strip()
                if not completed.strip():
                    continue
                parsed_fields = self._parse_admin_declaration_fields(completed)
                token_like = [t for t in re.split(r'\s+', completed.strip()) if re.fullmatch(r'[A-Za-z0-9_-]+', t)]
                if not parsed_fields and len(token_like) < 3 and not code:
                    continue
                return {
                    'ts_iso': to_beijing_time_text(fr.get('ts_iso')),
                    'peer_ip': ip,
                    'event_type': 'declaration_completion',
                    'source': 'TC_PUSH_RAW',
                    'payload': {
                        'code': code,
                        'code_str': code,
                        'completed_text': completed,
                        'parsed_fields': parsed_fields,
                    },
                }
        except Exception:
            return None
        return None

    @staticmethod
    def _parse_admin_declaration_fields(text: Optional[str]) -> Dict[str, str]:
        if not text:
            return {}
        compact = re.sub(r'\s+', ' ', str(text)).strip()
        out: Dict[str, str] = {}
        patterns = {
            'employee_id': r'Employee\s*ID\s*[:=]\s*([^:]+?)(?=\s+Production\s*Plan\s*ID\s*[:=]|\s+Yarn\s*Package\s*Number\s*[:=]|$)',
            'production_plan_id': r'Production\s*Plan\s*ID\s*[:=]\s*([^:]+?)(?=\s+Yarn\s*Package\s*Number\s*[:=]|$)',
            'yarn_package_number': r'Yarn\s*Package\s*Number\s*[:=]\s*(.+)$',
        }
        for key, pattern in patterns.items():
            m = re.search(pattern, compact, flags=re.IGNORECASE)
            if m:
                out[key] = m.group(1).strip(' _-:')
        if out:
            return out
        if compact.lower().startswith('msg:565'):
            tokens = [t for t in re.split(r'\s+', compact) if t and ':' not in t]
            if len(tokens) >= 3:
                return {
                    'employee_id': tokens[0],
                    'production_plan_id': tokens[1],
                    'yarn_package_number': tokens[2],
                }
        tokens = [t for t in re.split(r'\s+', compact) if t and t not in {'msg:565'}]
        nums = [t for t in tokens if re.fullmatch(r'[A-Za-z0-9_-]+', t)]
        if len(nums) >= 3:
            return {
                'employee_id': nums[-3],
                'production_plan_id': nums[-2],
                'yarn_package_number': nums[-1],
            }
        return out

    def _render_dashboard_cards(self, snapshot: Dict[str, Any]) -> str:
        cards: List[str] = []
        for ip, info in snapshot.items():
            last_status = info.get("last_status") or {}
            event = last_status.get("event") or {}
            interpreted = last_status.get("interpreted") or {}
            commands = info.get("commands") or {}
            speed_result = commands.get("speed") or {}
            picks_result = commands.get("total_picks") or {}
            density_result = commands.get("density") or {}
            pattern_current = commands.get("pattern_current") or {}
            preselection = commands.get("preselection") or {}
            basic_cfg = commands.get("basic_config") or {}
            lamp = commands.get("lamp_tree") or {}
            log_payload = commands.get("error_logs") or {}
            log_reason = self._latest_log_reason(log_payload)
            admin_event = self._latest_admin_declaration_event(ip)
            admin_payload = (admin_event or {}).get("payload") or {}
            admin_text = admin_payload.get("completed_text")
            admin_fields = self._parse_admin_declaration_fields(admin_text)

            running = bool(interpreted.get("running"))
            category = interpreted.get("category") or "unknown"
            detail = interpreted.get("detail")
            if detail in (None, "", "—"):
                detail = "none (running)" if running else "not reported"
            detail_display = detail
            if (not running) and log_reason:
                low_detail = str(detail).lower()
                if low_detail in {"mechanical", "operator_stop", "auxiliary_halt", "unknown_other_stop", "not reported"}:
                    detail_display = f"{detail} · {log_reason}"
            banner_text = ("Running" if running else f"{category.replace('_', ' ').title()} / {detail_display.replace('_', ' ')}")
            pill_class = "ok" if info.get("tcp_connected") else "warn"
            pill_text = "TC online" if info.get("tcp_connected") else "TC idle"

            speed_value = event.get("speed_rpm")
            if speed_value in (None, ""):
                speed_value = speed_result.get("value")
            picks_value = event.get("total_picks")
            if picks_value in (None, ""):
                picks_value = picks_result.get("value")
            density_value = event.get("density_weft_per_dm")
            if density_value in (None, ""):
                density_value = density_result.get("value")

            metrics = [
                self._metric_card("Speed", speed_value, "rpm"),
                self._metric_card("Picks", picks_value, "total"),
                self._metric_card("Efficiency", self._fmt_efficiency(event.get("efficiency_x100")), "current shift"),
                self._metric_card("Density", density_value, "weft/dm"),
            ]

            infos = [
                self._info_card("Status category", category),
                self._info_card("Status detail", detail_display),
                self._info_card("TS port", info.get("ts_port", core.DEFAULT_TS_PORT)),
                self._info_card("Current pattern", pattern_current.get("name")),
                self._info_card("Pattern step / colour", self._join_parts(pattern_current.get("step"), pattern_current.get("colour"))),
                self._info_card("Loom type / model", self._join_parts(basic_cfg.get("loom_type"), basic_cfg.get("loom_model"))),
                self._info_card("Software ID / version", basic_cfg.get("software_version")),
                self._info_card("Preselection", self._join_parts(preselection.get("preselection_value"), preselection.get("unit_text"))),
                self._info_card("Preselection counter", preselection.get("counter_value")),
                self._info_card("Lamp tree mask", lamp.get("mask_hex") or lamp.get("mask")),
                self._info_card("Warp tensions", self._join_parts(event.get("warp_tensions", [None, None, None])[0], event.get("warp_tensions", [None, None, None])[1], event.get("warp_tensions", [None, None, None])[2])),
                self._info_card("Last stop / error", log_reason),
            ]

            admin_infos = [
                self._info_card("Last administrative declaration", self._join_parts(admin_payload.get("code_str") or admin_payload.get("code"), (admin_event or {}).get("ts_iso")), "compact"),
                self._info_card("Employee ID", admin_fields.get("employee_id"), "compact"),
                self._info_card("Production Plan ID", admin_fields.get("production_plan_id"), "compact"),
                self._info_card("Yarn Package Number", admin_fields.get("yarn_package_number"), "compact"),
                self._info_card("Last update", to_beijing_time_text(last_status.get("updated_at_iso")) or "—", "compact"),
            ]

            chipline = [
                self._chip(f"TC: {'connected' if info.get('tcp_connected') else 'idle'}"),
                self._chip(f"Last TS poll {self._fmt_age(info.get('last_ts_poll_ts'))}"),
                self._chip(f"Last TC packet {self._fmt_age(info.get('last_tc_rx_ts'))}"),
            ]
            if basic_cfg.get("serial_number"):
                chipline.append(self._chip(f"Serial {basic_cfg.get('serial_number')}"))
            if density_result.get("unit_text"):
                chipline.append(self._chip(f"Density unit {density_result.get('unit_text')}"))
            if preselection.get("enabled") is not None:
                chipline.append(self._chip(f"Preselection {'ON' if preselection.get('enabled') else 'OFF'}"))

            actions = f"""
            <div class=\"actions\">
              <div class=\"action-row\"><div class=\"action-label\">Monitor</div>
                <button class=\"button\" onclick=\"readAllStatus('{self._escape(ip)}')\">Read All Status</button>
              </div>
              <div class=\"action-row\"><div class=\"action-label\">Pattern</div>
                <button class=\"button\" onclick=\"patternInfo('{self._escape(ip)}')\">Pattern Info</button>
                <button class=\"button warn\" onclick=\"promptPatternSend('{self._escape(ip)}')\">Send Pattern</button>
              </div>
              <div class=\"action-row\"><div class=\"action-label\">Machine</div>
                <button class=\"button warn\" onclick=\"promptPreselectionWrite('{self._escape(ip)}')\">Preselection Write</button>
                <button class=\"button ghost\" onclick=\"runProtectedSimple('preselection-reset','{self._escape(ip)}')\">Preselection Reset</button>
              </div>
              <div class=\"action-row\"><div class=\"action-label\">Operator</div>
                <button class=\"button alt\" onclick=\"promptAdminDeclaration('{self._escape(ip)}')\">Administrative Declaration</button>
                <button class=\"button warn\" onclick=\"promptLampWrite('{self._escape(ip)}')\">Lamp Write</button>
                <button class=\"button danger\" onclick=\"remoteStop('{self._escape(ip)}')\">Remote Stop</button>
              </div>
            </div>
            """

            cards.append(
                "<section class=\"loom-card\">"
                "<div class=\"card-top\">"
                f"<div><h2>{self._escape(self._display_loom_name(info.get('name') or ip))}</h2><div class=\"sub\">{self._escape(ip)}</div></div>"
                f"<div class=\"pill {pill_class}\">{self._escape(pill_text)}</div>"
                "</div>"
                f"<div class=\"banner {'running' if running else 'stopped'}\">{self._escape(banner_text)}</div>"
                f"{actions}"
                f"<div class=\"metrics-grid\">{''.join(metrics)}</div>"
                f"<div class=\"info-grid\">{''.join(infos)}</div>"
                f"<div class=\"admin-row\">{''.join(admin_infos)}</div>"
                f"<div class=\"chipline\">{''.join(chipline)}</div>"
                "</section>"
            )
        return ''.join(cards) if cards else '<section class="loom-card"><h2>No looms configured or seen yet.</h2></section>'

    def _render_dashboard_html(self) -> str:
        snapshot = self.app.dashboard_snapshot()
        cards_html = self._render_dashboard_cards(snapshot)
        body = f"""
        <div class=\"toolbar\">
          <button class=\"button\" onclick=\"refreshAll()\">Refresh all looms now</button>
        </div>
        <div class=\"cards\" id=\"cards-root\">{cards_html}</div>
        """
        return self._base_page("Loom Real-Time Monitoring Panel", "/", body)

    def _render_error_logs_section(self, snapshot: Dict[str, Any]) -> str:
        return ""

    def _render_machines_html(self) -> str:
        cfg_path = self.app.current_config_path() or "Not using an external config file"
        rows: List[str] = []
        for item in self.app.list_loom_configs():
            ip = item.get("ip") or ""
            name = self._display_loom_name(item.get("name") or ip)
            ts_port = int(item.get("ts_port", core.DEFAULT_TS_PORT))
            qt5 = bool(item.get("supports_qt5_full_status", False))
            enabled = bool(item.get("enabled", True))
            rows.append(
                "<tr>"
                f"<td>{self._escape(name)}</td>"
                f"<td><code>{self._escape(ip)}</code></td>"
                f"<td>{ts_port}</td>"
                f"<td>{'Yes' if qt5 else 'No'}</td>"
                f"<td>{'Yes' if enabled else 'No'}</td>"
                f"<td><button class=\"button ghost\" type=\"button\" onclick=\"promptMachineEdit('{self._escape(ip)}','{self._escape(name)}','{self._escape(ip)}',{ts_port},{'true' if qt5 else 'false'},{'true' if enabled else 'false'})\">Edit</button> "
                f"<button class=\"button danger\" type=\"button\" onclick=\"deleteMachine('{self._escape(ip)}','{self._escape(name)}')\">Delete</button></td>"
                "</tr>"
            )
        body = f"""
        <section class="section">
          <h2>Machine IP Management</h2>
          <div class="muted">Save loom IPs, names, and TS ports directly into the config file you started the program with.</div>
          <div class="toolbar">
            <button class="button alt" type="button" onclick="promptMachineAdd()">Add loom</button>
            <a class="button ghost" href="/">Back to dashboard</a>
          </div>
          <div class="muted">Config file: {self._escape(cfg_path)}</div>
          <table>
            <thead><tr><th>Name</th><th>IP</th><th>TS Port</th><th>QT5 Full Status</th><th>Enabled</th><th>Actions</th></tr></thead>
            <tbody>{''.join(rows) if rows else '<tr><td colspan="6">No looms are configured yet.</td></tr>'}</tbody>
          </table>
        </section>
        """
        scripts = """
    async function promptMachineAdd() {
      const data = await openModal({
        title: 'Add Loom',
        subtitle: 'Create a new loom entry in the config file.',
        okText: 'Add loom',
        fields: [
          { name: 'name', label: 'Loom name', value: '' },
          { name: 'new_ip', label: 'Loom IP', value: '' },
          { name: 'ts_port', label: 'TS Port', value: '13000' },
          { name: 'supports_qt5_full_status', label: 'QT5 full status', type: 'checkbox', value: true },
          { name: 'enabled', label: 'Enabled on dashboard', type: 'checkbox', value: true },
        ]
      });
      if (!data) return;
      try {
        await guardedPostForm('/action/machine-add', {
          name: data.name || '',
          new_ip: data.new_ip || '',
          ts_port: data.ts_port || '13000',
          supports_qt5_full_status: data.supports_qt5_full_status || '0',
          enabled: data.enabled || '0',
        });
        showToast('ok', 'Loom added');
        setTimeout(() => location.reload(), 250);
      } catch (err) {
        showToast('error', `Add loom failed: ${err.message || err}`);
      }
    }
    async function promptMachineEdit(oldIp, currentName, currentIp, tsPort, qt5, enabled) {
      const data = await openModal({
        title: 'Edit Loom',
        subtitle: currentIp,
        okText: 'Save changes',
        fields: [
          { name: 'name', label: 'Loom name', value: currentName || '' },
          { name: 'new_ip', label: 'Loom IP', value: currentIp || '' },
          { name: 'ts_port', label: 'TS Port', value: String(tsPort || 13000) },
          { name: 'supports_qt5_full_status', label: 'QT5 full status', type: 'checkbox', value: !!qt5 },
          { name: 'enabled', label: 'Enabled on dashboard', type: 'checkbox', value: !!enabled },
        ]
      });
      if (!data) return;
      try {
        await guardedPostForm('/action/machine-save', {
          old_ip: oldIp || '',
          name: data.name || '',
          new_ip: data.new_ip || '',
          ts_port: data.ts_port || '13000',
          supports_qt5_full_status: data.supports_qt5_full_status || '0',
          enabled: data.enabled || '0',
        });
        showToast('ok', 'Loom updated');
        setTimeout(() => location.reload(), 250);
      } catch (err) {
        showToast('error', `Save loom failed: ${err.message || err}`);
      }
    }
    async function deleteMachine(loomIp, loomName) {
      const data = await openModal({
        title: 'Delete Loom',
        subtitle: loomIp,
        okText: 'Delete',
        okClass: 'danger',
        message: `Delete loom ${loomName || loomIp}? This updates the config file immediately.`,
      });
      if (!data) return;
      try {
        await guardedPostForm('/action/machine-delete', { loom_ip: loomIp || '' });
        showToast('ok', 'Loom deleted');
        setTimeout(() => location.reload(), 250);
      } catch (err) {
        showToast('error', `Delete loom failed: ${err.message || err}`);
      }
    }
        """
        return self._base_page("Loom Real-Time Monitoring Panel · Machine IPs", "/machines", body, scripts=scripts)

    def _render_capabilities_html(self) -> str:
        return self._base_page("Loom Real-Time Monitoring Panel · Capabilities", "/capabilities", "<section class='section'><h2>Capabilities</h2><div class='muted'>Capability view is reserved for future expansion.</div></section>")

    def _render_live_html(self) -> str:
        events = self.store.recent_events(100) if self.store else []
        rows = []
        for ev in events:
            rows.append(
                "<tr>"
                f"<td>{self._escape(to_beijing_time_text(ev.get('ts_iso')))}</td>"
                f"<td>{self._escape(ev.get('peer_ip'))}</td>"
                f"<td>{self._escape(ev.get('event_type'))}</td>"
                f"<td><pre>{self._escape(json.dumps(ev.get('payload'), ensure_ascii=False, indent=2))}</pre></td>"
                "</tr>"
            )
        body = f"""
        <section class="section">
          <h2>Recent Events</h2>
          <table>
            <thead><tr><th>Time</th><th>Peer IP</th><th>Event</th><th>Payload</th></tr></thead>
            <tbody>{''.join(rows) if rows else '<tr><td colspan="4">No events captured yet.</td></tr>'}</tbody>
          </table>
        </section>
        """
        return self._base_page("Loom Real-Time Monitoring Panel · Live Events", "/live", body)

    def _metric_card(self, label: str, value: Any, sub: str = "") -> str:
        rendered = "—" if value in (None, "") else self._escape(value)
        sub_html = f'<div class="metric-sub">{self._escape(sub)}</div>' if sub else ""
        return f'<div class="metric-tile"><div class="metric-label">{self._escape(label)}</div><div class="metric-value">{rendered}</div>{sub_html}</div>'

    def _info_card(self, label: str, value: Any, extra_class: str = "") -> str:
        rendered = "—" if value in (None, "") else self._escape(value)
        extra = f" {extra_class}" if extra_class else ""
        return f'<div class="info-card{extra}"><div class="info-label">{self._escape(label)}</div><div class="info-value">{rendered}</div></div>'

    def _chip(self, text: str) -> str:
        return f'<div class="chip">{self._escape(text)}</div>'

    @staticmethod
    def _join_parts(*parts: Any) -> str:
        cleaned = [str(p) for p in parts if p not in (None, "", [], {})]
        return " · ".join(cleaned) if cleaned else "—"


class MasterApp(core.LoomHostApp):
    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.dashboard = InteractiveDashboardServer(self, self.registry, self.sink, self.store, self.log)
        self.command_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.command_support: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.admin_declarations: Dict[str, Dict[str, Any]] = {}
        self.last_admin_completions: Dict[str, Dict[str, Any]] = {}
        _orig_log_event = self.sink.log_event
        def _wrapped_log_event(peer_ip: str, event_type: str, source: str, payload: Dict[str, Any]) -> None:
            if event_type == 'declaration_completion':
                self.last_admin_completions[peer_ip] = {
                    'ts_iso': bj_now_iso(),
                    'peer_ip': peer_ip,
                    'event_type': event_type,
                    'source': source,
                    'payload': dict(payload or {}),
                }
            _orig_log_event(peer_ip, event_type, source, payload)
        self.sink.log_event = _wrapped_log_event

    def current_config_path(self) -> Optional[str]:
        return self.config.get("__config_path")

    def list_loom_configs(self) -> List[Dict[str, Any]]:
        items = []
        for item in self.config.get("looms", []):
            row = dict(item)
            row.setdefault("enabled", True)
            items.append(row)
        return items

    def is_loom_enabled(self, loom_ip: str) -> bool:
        cfg = self.registry.config_by_ip.get(loom_ip) or {}
        return bool(cfg.get("enabled", True))

    def _write_runtime_config(self) -> None:
        config_path = self.current_config_path()
        if not config_path:
            raise ValueError("No external config file is active; start the program with --config to enable saving.")
        path = Path(config_path)
        payload = dict(self.config)
        payload.pop("__config_path", None)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    def upsert_loom_config(
        self,
        old_ip: Optional[str],
        name: str,
        new_ip: str,
        ts_port: int,
        supports_qt5_full_status: bool,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        new_ip = str(new_ip or "").strip()
        if not new_ip:
            raise ValueError("new_ip is required")
        if ts_port <= 0 or ts_port > 65535:
            raise ValueError("ts_port must be between 1 and 65535")
        name = (name or "").strip() or f"loom-{new_ip}"
        looms = self.config.setdefault("looms", [])

        existing_idx = None
        old_item = None
        if old_ip:
            for idx, item in enumerate(looms):
                if str(item.get("ip")) == old_ip:
                    existing_idx = idx
                    old_item = dict(item)
                    break

        # prevent duplicate IP when editing/adding
        for idx, item in enumerate(looms):
            if str(item.get("ip")) == new_ip and idx != existing_idx:
                raise ValueError(f"A loom with IP {new_ip} already exists")

        updated = dict(old_item or {})
        updated.update({
            "name": name,
            "ip": new_ip,
            "ts_port": int(ts_port),
            "supports_qt5_full_status": bool(supports_qt5_full_status),
            "enabled": bool(enabled),
        })
        updated.setdefault("declarations", (old_item or {}).get("declarations", {}))

        if existing_idx is None:
            looms.append(updated)
        else:
            looms[existing_idx] = updated

        # update runtime config mappings
        if old_ip and old_ip != new_ip:
            self.registry.config_by_ip.pop(old_ip, None)
            self.command_cache[new_ip] = self.command_cache.pop(old_ip, {})
            self.command_support[new_ip] = self.command_support.pop(old_ip, {})
            self.admin_declarations[new_ip] = self.admin_declarations.pop(old_ip, {})
            self.last_admin_completions[new_ip] = self.last_admin_completions.pop(old_ip, {})
        self.registry.config_by_ip[new_ip] = updated

        self._write_runtime_config()
        return {"ok": True, "loom": updated, "config_path": self.current_config_path()}

    def delete_loom_config(self, loom_ip: str) -> Dict[str, Any]:
        loom_ip = str(loom_ip or "").strip()
        if not loom_ip:
            raise ValueError("loom_ip is required")
        looms = self.config.setdefault("looms", [])
        new_looms = [item for item in looms if str(item.get("ip")) != loom_ip]
        if len(new_looms) == len(looms):
            raise ValueError(f"No configured loom found for IP {loom_ip}")
        self.config["looms"] = new_looms
        self.registry.config_by_ip.pop(loom_ip, None)
        self.command_cache.pop(loom_ip, None)
        self.command_support.pop(loom_ip, None)
        self.admin_declarations.pop(loom_ip, None)
        self.last_admin_completions.pop(loom_ip, None)
        self._write_runtime_config()
        return {"ok": True, "deleted_ip": loom_ip, "config_path": self.current_config_path()}

    def configure_admin_declaration(self, loom_ip: str, code_str: str, template: str) -> Dict[str, Any]:
        code = str(code_str or '').strip()
        if not re.fullmatch(r"\d{3}", code):
            raise ValueError("code_str must be exactly 3 digits")
        tpl = str(template or '').strip()
        if not tpl:
            raise ValueError("template is required")
        cfg = self.registry.config_by_ip.setdefault(loom_ip, {"name": f"unknown-{loom_ip}", "ip": loom_ip, "declarations": {}})
        decls = cfg.setdefault("declarations", {})
        # Prefix the declaration code to the returned template text, as required by the loom's declaration workflow.
        wire_tpl = tpl if tpl.startswith(code) else (code + tpl)
        decls[code] = wire_tpl
        payload = {"code_str": code, "template": tpl, "wire_template": wire_tpl, "updated_at_iso": bj_now_iso()}
        self.admin_declarations[loom_ip] = payload
        self._remember(loom_ip, "admin_declaration", {"loom_ip": loom_ip, **payload}, summary=f"Administrative declaration {code} configured")
        return payload

    def _port_for(self, loom_ip: str) -> int:
        cfg = self.registry.config_by_ip.get(loom_ip, {})
        return int(cfg.get("ts_port", core.DEFAULT_TS_PORT))

    def dashboard_snapshot(self) -> Dict[str, Any]:
        base = self.registry.snapshot()
        merged: Dict[str, Any] = {}
        for ip, item in base.items():
            if not self.is_loom_enabled(ip):
                continue
            merged[ip] = dict(item)
            merged[ip]["commands"] = self.command_cache.get(ip, {})
            merged[ip]["support"] = self.command_support.get(ip, {})
        for ip in self.command_cache:
            if not self.is_loom_enabled(ip):
                continue
            if ip not in merged:
                merged[ip] = {
                    "name": ip,
                    "ip": ip,
                    "ts_port": self._port_for(ip),
                    "tcp_connected": False,
                    "current_peer_port": None,
                    "last_tc_rx_ts": 0.0,
                    "last_ts_poll_ts": 0.0,
                    "last_status": None,
                    "supports_qt5_full_status": False,
                    "commands": self.command_cache.get(ip, {}),
                    "support": self.command_support.get(ip, {}),
                }
        return merged

    def _remember(self, loom_ip: str, key: str, payload: Dict[str, Any], summary: Optional[str] = None) -> None:
        self.command_cache.setdefault(loom_ip, {})[key] = dict(payload)
        if summary:
            self.log.info("%s %s", loom_ip, summary)

    async def refresh_dashboard(self, force: bool = False) -> None:
        for loom in self.registry.all_known_looms():
            if not self.is_loom_enabled(loom.ip):
                continue
            try:
                await self.query_all_status(loom.ip, force=force)
            except Exception as exc:
                self.log.warning("dashboard refresh failed for %s: %s", loom.ip, exc)

    async def query_status(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), core.encode_status_request())
        event = core.decode_status(frame.payload, source="TS_REPLY")
        self.registry.mark_ts_poll(loom_ip)
        self.registry.set_last_status(loom_ip, event)
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "decoded": {"event": core.asdict(event), "interpreted": core.interpret_status(event)}}
        self._remember(loom_ip, "status", payload, summary="TS status refreshed")
        return payload

    async def query_full_status(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), core.encode_full_status_request())
        event = core.decode_full_status(frame.payload, source="TS_REPLY")
        self.registry.mark_ts_poll(loom_ip)
        self.registry.set_last_status(loom_ip, event)
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "decoded": {"event": core.asdict(event), "interpreted": core.interpret_status(event)}}
        self._remember(loom_ip, "full_status", payload, summary="TS full-status refreshed")
        return payload

    async def query_speed(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), core.encode_speed_request())
        value = core.u16(frame.payload[0], frame.payload[1]) if len(frame.payload) >= 2 else None
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "value": value, "unit": "rpm"}
        self._remember(loom_ip, "speed", payload)
        return payload

    async def query_pattern_current(self, loom_ip: str) -> Dict[str, Any]:
        req = bytes([CMD_PATTERN, 0x00])
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), req)
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex()}
        if frame.length >= 3:
            fmt = frame.payload[0]
            step = core.u16(frame.payload[1], frame.payload[2])
            colour = frame.payload[3] if frame.length >= 4 else None
            name = frame.payload[4:].split(b"\x00", 1)[0].decode("ascii", errors="ignore") if frame.length > 4 else None
            payload.update({"format": PATTERN_FORMAT.get(fmt, f"0x{fmt:02X}"), "step": step, "colour": colour, "name": name})
        self._remember(loom_ip, "pattern_current", payload)
        return payload

    async def query_pattern_info(self, loom_ip: str, pattern_name: str) -> Dict[str, Any]:
        raw = pattern_name.encode("ascii", errors="strict") + b"\x00"
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_PATTERN, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "pattern_name": pattern_name, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "pattern_info", payload)
        return payload

    async def send_pattern(self, loom_ip: str, pattern_name: str) -> Dict[str, Any]:
        raw = pattern_name.encode("ascii", errors="strict") + b"\x00"
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_PATTERN, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "pattern_name": pattern_name, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "pattern_send", payload)
        return payload

    async def query_basic_config(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_BASIC_CONFIG, 0x00]))
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex()}
        data = frame.payload
        try:
            text = data.decode("ascii", errors="ignore").replace("\x00", " ").strip()
            parts = [p for p in re.split(r'\s+', text) if p]
            payload["text"] = text
            if parts:
                payload["software_version"] = parts[0]
            if len(parts) >= 2:
                payload["loom_type"] = parts[1]
            if len(parts) >= 3:
                payload["loom_model"] = parts[2]
            if len(parts) >= 4:
                payload["serial_number"] = parts[3]
        except Exception:
            pass
        self._remember(loom_ip, "basic_config", payload)
        return payload

    async def query_total_picks(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_TOTAL_PICKS, 0x00]))
        value = core.u32(frame.payload[0], frame.payload[1], frame.payload[2], frame.payload[3]) if len(frame.payload) >= 4 else None
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "value": value}
        self._remember(loom_ip, "total_picks", payload)
        return payload

    async def query_density(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_DENSITY, 0x00]))
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex()}
        if len(frame.payload) >= 3:
            unit = frame.payload[0]
            value = core.u16(frame.payload[1], frame.payload[2])
            payload.update({"unit": unit, "unit_text": UNIT_DENSITY.get(unit, f"unit_{unit}"), "value": value})
        self._remember(loom_ip, "density", payload)
        return payload

    async def query_lamp_tree(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_LAMP, 0x00]))
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex()}
        if len(frame.payload) >= 4:
            mask = core.u32(frame.payload[0], frame.payload[1], frame.payload[2], frame.payload[3])
            payload.update({"mask": mask, "mask_hex": f"0x{mask:08X}"})
        self._remember(loom_ip, "lamp_tree", payload)
        return payload

    async def write_lamp_tree(self, loom_ip: str, calls_mask: int) -> Dict[str, Any]:
        raw = calls_mask.to_bytes(4, "big", signed=False)
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_LAMP, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "calls_mask": calls_mask, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "lamp_write", payload)
        return payload

    async def query_preselection(self, loom_ip: str) -> Dict[str, Any]:
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_PRESELECTION, 0x00]))
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex()}
        if len(frame.payload) >= 6:
            flags = frame.payload[0]
            unit = frame.payload[1]
            preselection_value = core.u16(frame.payload[2], frame.payload[3])
            counter_value = core.u16(frame.payload[4], frame.payload[5])
            payload.update({
                "enabled": bool(flags & 0x01),
                "stop_on_reach": bool(flags & 0x02),
                "confirm_before_restart": bool(flags & 0x04),
                "delayed_stop": bool(flags & 0x08),
                "manual_reset": bool(flags & 0x10),
                "unit": unit,
                "unit_text": UNIT_PRESELECTION.get(unit, f"unit_{unit}"),
                "preselection_value": preselection_value,
                "counter_value": counter_value,
            })
        self._remember(loom_ip, "preselection", payload)
        return payload

    async def write_preselection(self, loom_ip: str, settings: Dict[str, Any]) -> Dict[str, Any]:
        flags = 0
        if settings.get("enabled"):
            flags |= 0x01
        if settings.get("stop_on_reach"):
            flags |= 0x02
        if settings.get("confirm_before_restart"):
            flags |= 0x04
        if settings.get("delayed_stop"):
            flags |= 0x08
        if settings.get("manual_reset"):
            flags |= 0x10
        unit = int(settings.get("unit", 1)) & 0xFF
        preselection_value = int(settings.get("preselection_value", 0)) & 0xFFFF
        counter_value = int(settings.get("counter_value", 0)) & 0xFFFF
        raw = bytes([flags, unit, (preselection_value >> 8) & 0xFF, preselection_value & 0xFF, (counter_value >> 8) & 0xFF, counter_value & 0xFF])
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_PRESELECTION, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "settings": settings, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "preselection_write", payload)
        return payload

    async def reset_preselection(self, loom_ip: str) -> Dict[str, Any]:
        raw = bytes([0xFF])
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_PRESELECTION, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "preselection_reset", payload)
        return payload

    async def remote_stop(self, loom_ip: str) -> Dict[str, Any]:
        raw = bytes([0x01])
        frame = await self.ts_client.transact(loom_ip, self._port_for(loom_ip), bytes([CMD_REMOTE_CONTROL, len(raw)]) + raw)
        result_code = frame.payload[0] if frame.payload else None
        payload = {"loom_ip": loom_ip, "cmd": frame.cmd, "length": frame.length, "raw_hex": frame.as_hex(), "result_code": result_code, "result_text": RESULT_TEXT.get(result_code, f"0x{result_code:02X}" if result_code is not None else "no data")}
        self._remember(loom_ip, "remote_stop", payload)
        return payload

    async def query_error_logs(self, loom_ip: str) -> Dict[str, Any]:
        host = loom_ip
        port = int(self.registry.config_by_ip.get(loom_ip, {}).get("ftp_port", DEFAULT_FTP_PORT))
        user = self.registry.config_by_ip.get(loom_ip, {}).get("ftp_user", DEFAULT_FTP_USER)
        password = self.registry.config_by_ip.get(loom_ip, {}).get("ftp_password", DEFAULT_FTP_PASSWORD)
        app_dir = self.registry.config_by_ip.get(loom_ip, {}).get("ftp_app_dir", DEFAULT_FTP_APP_DIR)
        errors_name = self.registry.config_by_ip.get(loom_ip, {}).get("ftp_errors_file", "errors.csv")
        events_name = self.registry.config_by_ip.get(loom_ip, {}).get("ftp_events_file", "events.csv")

        def _fetch() -> Dict[str, Any]:
            result: Dict[str, Any] = {"loom_ip": loom_ip, "ftp": {"host": host, "port": port, "user": user, "app_dir": app_dir}, "errors": {}, "events": {}}
            with ftplib.FTP() as ftp:
                ftp.connect(host, port, timeout=5)
                ftp.login(user=user, passwd=password)
                if app_dir:
                    ftp.cwd(app_dir)
                for key, filename in (("errors", errors_name), ("events", events_name)):
                    buf = io.BytesIO()
                    ftp.retrbinary(f"RETR {filename}", buf.write)
                    raw = buf.getvalue().decode("utf-8", errors="replace")
                    rows = list(csv.reader(io.StringIO(raw)))
                    result[key] = {"filename": filename, "raw": raw, "rows": rows}
            return result

        payload = await asyncio.to_thread(_fetch)
        self._remember(loom_ip, "error_logs", payload)
        return payload

    async def query_all_status(self, loom_ip: str, force: bool = False) -> Dict[str, Any]:
        results: Dict[str, Any] = {"loom_ip": loom_ip}
        try:
            results["status"] = await self.query_status(loom_ip)
        except Exception as exc:
            results["status"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        cfg = self.registry.config_by_ip.get(loom_ip, {})
        if bool(cfg.get("supports_qt5_full_status", False)):
            try:
                results["full_status"] = await self.query_full_status(loom_ip)
            except Exception as exc:
                results["full_status"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        for key, fn in [
            ("speed", self.query_speed),
            ("basic_config", self.query_basic_config),
            ("total_picks", self.query_total_picks),
            ("density", self.query_density),
            ("pattern_current", self.query_pattern_current),
            ("preselection", self.query_preselection),
            ("lamp_tree", self.query_lamp_tree),
        ]:
            try:
                results[key] = await fn(loom_ip)
            except Exception as exc:
                results[key] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        return results


def load_config(path: Optional[str]) -> Dict[str, Any]:
    if path is None:
        return core.default_config()
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    base = core.default_config()
    base.update(cfg)
    for item in base.get("looms", []):
        item.setdefault("enabled", True)
    base["__config_path"] = str(Path(path).resolve())
    return base


def setup_logging(level_name: str) -> None:
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


async def tcp_connectivity_test(ip: str, port: int, timeout_s: float = 2.0) -> Dict[str, Any]:
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout=timeout_s)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        return {"ok": True, "ip": ip, "port": port}
    except Exception as exc:
        return {"ok": False, "ip": ip, "port": port, "error": f"{type(exc).__name__}: {exc}"}


async def bind_test(host: str, port: int, udp: bool) -> Dict[str, Any]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM if udp else socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        if not udp:
            sock.listen(1)
        return {"ok": True, "host": host, "port": port, "transport": "udp" if udp else "tcp"}
    except Exception as exc:
        return {"ok": False, "host": host, "port": port, "transport": "udp" if udp else "tcp", "error": f"{type(exc).__name__}: {exc}"}
    finally:
        sock.close()


async def run_service(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    app = MasterApp(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, app.request_stop)
        except NotImplementedError:
            pass

    background_tasks: List[asyncio.Task[Any]] = []
    if args.snapshot_interval > 0:
        background_tasks.append(asyncio.create_task(app.print_snapshot_loop(args.snapshot_interval)))

    try:
        await app.run_until_stopped()
    finally:
        for task in background_tasks:
            task.cancel()
        await asyncio.gather(*background_tasks, return_exceptions=True)
        await app.stop()
    return 0


async def run_selftest(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    host = config.get("listen_host", "0.0.0.0")
    host_port = int(config.get("host_port", core.DEFAULT_HOST_PORT))
    report: Dict[str, Any] = {
        "listener_tcp_bind": await bind_test(host, host_port, udp=False),
        "listener_udp_bind": await bind_test(host, host_port, udp=True),
        "looms": [],
    }

    app = MasterApp(config)
    for loom in config.get("looms", []):
        ip = loom["ip"]
        ts_port = int(loom.get("ts_port", core.DEFAULT_TS_PORT))
        entry: Dict[str, Any] = {
            "name": loom.get("name", ip),
            "ip": ip,
            "ts_connectivity": await tcp_connectivity_test(ip, ts_port, timeout_s=float(config.get("ts_connect_timeout_seconds", 3.0))),
        }
        try:
            entry["status_query"] = await app.query_status(ip)
        except Exception as exc:
            entry["status_query"] = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        report["looms"].append(entry)
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


async def run_query(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    app = MasterApp(config)
    method = args.action
    try:
        if method == "status":
            result = await app.query_status(args.loom_ip)
        elif method == "full-status":
            result = await app.query_full_status(args.loom_ip)
        elif method == "speed":
            result = await app.query_speed(args.loom_ip)
        elif method == "basic-config":
            result = await app.query_basic_config(args.loom_ip)
        elif method == "total-picks":
            result = await app.query_total_picks(args.loom_ip)
        elif method == "density":
            result = await app.query_density(args.loom_ip)
        elif method == "pattern-current":
            result = await app.query_pattern_current(args.loom_ip)
        elif method == "pattern-info":
            result = await app.query_pattern_info(args.loom_ip, args.pattern_name)
        elif method == "pattern-send":
            result = await app.send_pattern(args.loom_ip, args.pattern_name)
        elif method == "lamp-read":
            result = await app.query_lamp_tree(args.loom_ip)
        elif method == "lamp-write":
            result = await app.write_lamp_tree(args.loom_ip, int(args.calls_mask, 0))
        elif method == "preselection-read":
            result = await app.query_preselection(args.loom_ip)
        elif method == "preselection-reset":
            result = await app.reset_preselection(args.loom_ip)
        elif method == "remote-stop":
            result = await app.remote_stop(args.loom_ip)
        elif method == "popup":
            result = {"todo": "popup action handled by loom_host_v2 core declaration/popup facilities if needed", "message": args.message}
        else:
            raise ValueError(f"unsupported action: {method}")
        print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        return 0
    finally:
        await app.stop()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Autonomous SMIT loom host/master with dashboard")
    parser.add_argument("--config", default=None, help="Path to JSON config file")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    parser.add_argument("--snapshot-interval", type=float, default=0.0, help="Periodic runtime snapshot seconds (service mode)")

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run", help="Run full host service and dashboard")
    sub.add_parser("selftest", help="Run listener bind + TS connectivity tests")

    def add_loom_cmd(name: str, help_text: str, need_pattern: bool = False, need_mask: bool = False, need_message: bool = False) -> None:
        p = sub.add_parser(name, help=help_text)
        p.add_argument("--loom-ip", required=True)
        if need_pattern:
            p.add_argument("--pattern-name", required=True)
        if need_mask:
            p.add_argument("--calls-mask", required=True)
        if need_message:
            p.add_argument("--message", required=True)
        p.set_defaults(action=name)

    for name, desc in [
        ("status", "Read 0x0A status"),
        ("full-status", "Read 0x0C full status"),
        ("speed", "Read 0x32 speed"),
        ("basic-config", "Read 0x1B basic config"),
        ("total-picks", "Read 0x21 total picks"),
        ("density", "Read 0x96 density"),
        ("pattern-current", "Read current pattern"),
        ("lamp-read", "Read lamp tree mask"),
        ("preselection-read", "Read preselection state"),
        ("preselection-reset", "Reset preselection counter"),
        ("remote-stop", "Send remote stop"),
    ]:
        add_loom_cmd(name, desc)
    add_loom_cmd("pattern-info", "Read pattern info by name", need_pattern=True)
    add_loom_cmd("pattern-send", "Send/select pattern by name", need_pattern=True)
    add_loom_cmd("lamp-write", "Write lamp tree mask", need_mask=True)
    add_loom_cmd("popup", "Send popup message", need_message=True)
    return parser


async def async_main(args: argparse.Namespace) -> int:
    if args.command == "run":
        return await run_service(args)
    if args.command == "selftest":
        return await run_selftest(args)
    args.action = args.command
    return await run_query(args)


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
