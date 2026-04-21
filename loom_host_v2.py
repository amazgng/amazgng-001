#!/usr/bin/env python3
"""
Enhanced SMIT loom host implementation.

Adds to the original version:
- SQLite persistence for raw frames and decoded events
- Minimal built-in HTTP dashboard and JSON API
- Same TCP/UDP/TS protocol handling as the original script

Usage:
    python loom_host_v2.py --config loom_host_v2_config.example.json --log-level INFO

Key HTTP endpoints:
    GET /                 -> HTML dashboard
    GET /health           -> plain-text OK
    GET /api/looms        -> JSON snapshot of current loom state
    GET /api/events       -> JSON list of recent decoded events
    GET /api/frames       -> JSON list of recent raw frames
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import logging
import signal
import socket
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

# -----------------------------
# Constants / protocol commands
# -----------------------------
CMD_STATUS = 0x0A              # 10 decimal
CMD_COMPLETE_STATUS = 0x0B     # 11 decimal
CMD_FULL_STATUS = 0x0C         # 12 decimal
CMD_DECLARATION = 0x14         # 20 decimal
CMD_POPUP = 0x17               # 23 decimal
CMD_LIGHTS = 0x28              # 40 decimal
CMD_SPEED = 0x32               # 50 decimal

DEFAULT_TS_PORT = 13000
DEFAULT_HOST_PORT = 13001
DEFAULT_HTTP_PORT = 18080
MAX_STR_LEN = 255


class ProtocolError(Exception):
    pass


# -----------------------------
# Data models
# -----------------------------
@dataclass
class Frame:
    cmd: int
    length: int
    payload: bytes
    trailer: bytes = b""
    transport: str = "tcp"
    direction: str = "rx"
    peer_ip: str = ""
    peer_port: int = 0
    recv_ts: float = field(default_factory=time.time)

    def as_hex(self) -> str:
        return (bytes([self.cmd, self.length]) + self.payload + self.trailer).hex().upper()


@dataclass
class StatusEvent:
    stat1: int
    stat2: int
    stat3: int
    source: str


@dataclass
class CompleteStatusEvent:
    stat1: int
    stat2: int
    stat3: int
    speed_rpm: int
    total_picks: int
    source: str


@dataclass
class FullStatusI40Event:
    stat1: int
    stat2: int
    stat3: int
    speed_rpm: int
    total_picks: int
    shift: int
    efficiency_x100: int
    alarm_code: int
    density_weft_per_dm: int
    warp_tensions: List[int]
    meters: int
    source: str


@dataclass
class DeclarationRequest:
    code_str: str
    completed_text: Optional[str] = None


@dataclass
class LoomRuntimeState:
    name: str
    ip: str
    ts_port: int = DEFAULT_TS_PORT
    last_ts_poll_ts: float = 0.0
    last_tc_rx_ts: float = 0.0
    last_status: Optional[Dict[str, Any]] = None
    tcp_connected: bool = False
    current_peer_port: Optional[int] = None
    supports_qt5_full_status: bool = False


# -----------------------------
# Utilities
# -----------------------------
def u16(msb: int, lsb: int) -> int:
    return (msb << 8) | lsb


def u32(b3: int, b2: int, b1: int, b0: int) -> int:
    return (b3 << 24) | (b2 << 16) | (b1 << 8) | b0


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_ascii_nul_terminated(text: str) -> bytes:
    raw = text.encode("ascii", errors="strict") + b"\x00"
    if len(raw) > MAX_STR_LEN:
        raise ValueError(f"string too long: {len(raw)} > {MAX_STR_LEN}")
    return raw


# -----------------------------
# Parser
# -----------------------------
class LoomStreamParser:
    def __init__(self) -> None:
        self._state = "WAIT_CMD"
        self._cmd: Optional[int] = None
        self._length: Optional[int] = None
        self._payload = bytearray()

    def feed(self, data: bytes) -> List[Frame]:
        out: List[Frame] = []
        i = 0
        while i < len(data):
            if self._state == "WAIT_CMD":
                self._cmd = data[i]
                i += 1
                self._state = "WAIT_LEN"
            elif self._state == "WAIT_LEN":
                self._length = data[i]
                i += 1
                self._payload.clear()
                if self._length == 0:
                    out.append(Frame(cmd=self._cmd or 0, length=0, payload=b""))
                    self._state = "WAIT_CMD"
                else:
                    self._state = "WAIT_PAYLOAD"
            elif self._state == "WAIT_PAYLOAD":
                assert self._length is not None
                need = self._length - len(self._payload)
                take = min(need, len(data) - i)
                self._payload.extend(data[i:i + take])
                i += take
                if len(self._payload) == self._length:
                    trailer = b""
                    if self._cmd == CMD_DECLARATION and self._length == 3:
                        if i < len(data) and data[i] == 0x0A:
                            trailer = b"\x0A"
                            i += 1
                    out.append(Frame(
                        cmd=self._cmd or 0,
                        length=self._length,
                        payload=bytes(self._payload),
                        trailer=trailer,
                    ))
                    self._state = "WAIT_CMD"
            else:
                raise RuntimeError(f"invalid parser state: {self._state}")
        return out


# -----------------------------
# Encoder helpers
# -----------------------------
def encode_status_request() -> bytes:
    return bytes([CMD_STATUS, 0x00])


def encode_full_status_request() -> bytes:
    return bytes([CMD_FULL_STATUS, 0x00])


def encode_speed_request() -> bytes:
    return bytes([CMD_SPEED, 0x00])


def encode_declaration_reply(text: str) -> bytes:
    raw = ensure_ascii_nul_terminated(text)
    return bytes([CMD_DECLARATION, len(raw)]) + raw


def encode_popup_message(text: str) -> bytes:
    raw = ensure_ascii_nul_terminated(text)
    return bytes([CMD_POPUP, len(raw)]) + raw


# -----------------------------
# Decoders
# -----------------------------
def decode_status(payload: bytes, *, source: str) -> StatusEvent:
    if len(payload) < 3:
        raise ProtocolError(f"short 0x0A status payload: {len(payload)}")
    return StatusEvent(stat1=payload[0], stat2=payload[1], stat3=payload[2], source=source)


def decode_complete_status(payload: bytes, *, source: str) -> CompleteStatusEvent:
    if len(payload) != 9:
        raise ProtocolError(f"invalid 0x0B payload length: {len(payload)} != 9")
    return CompleteStatusEvent(
        stat1=payload[0],
        stat2=payload[1],
        stat3=payload[2],
        speed_rpm=u16(payload[3], payload[4]),
        total_picks=u32(payload[5], payload[6], payload[7], payload[8]),
        source=source,
    )


def decode_full_status(payload: bytes, *, source: str) -> FullStatusI40Event:
    payload_len = len(payload)
    if payload_len not in (0x1A, 0x1C):
        raise ProtocolError(f"invalid 0x0C payload length: {payload_len} (supported: 26 or 28)")

    # Two observed variants:
    # - 28-byte QT5-style frame, with 2 reserved bytes before meters
    # - 26-byte variant (seen on some looms / software families such as 5Plane),
    #   where the reserved bytes are omitted and meters starts immediately at byte 22.
    meters_offset = 24 if payload_len == 0x1C else 22

    return FullStatusI40Event(
        stat1=payload[0],
        stat2=payload[1],
        stat3=payload[2],
        speed_rpm=u16(payload[3], payload[4]),
        total_picks=u32(payload[5], payload[6], payload[7], payload[8]),
        shift=payload[9],
        efficiency_x100=u16(payload[10], payload[11]),
        alarm_code=u16(payload[12], payload[13]),
        density_weft_per_dm=u16(payload[14], payload[15]),
        warp_tensions=[u16(payload[16], payload[17]), u16(payload[18], payload[19]), u16(payload[20], payload[21])],
        meters=u32(payload[meters_offset], payload[meters_offset + 1], payload[meters_offset + 2], payload[meters_offset + 3]),
        source=source,
    )


def decode_declaration_request(frame: Frame) -> DeclarationRequest:
    if frame.length < 1:
        raise ProtocolError(f"declaration request too short: {frame.length}")

    # Some looms send a strict 3-digit request code (e.g. "565"),
    # while others send the completed declaration text back with or without
    # the 3-digit code prefix. Be permissive here so the dashboard can still
    # capture the completion.
    try:
        text = frame.payload.decode("ascii", errors="ignore")
    except Exception as exc:
        raise ProtocolError(f"declaration payload decode failed: {exc}") from exc

    text = text.rstrip("\x00\r\n")
    compact = text.strip()
    if not compact:
        raise ProtocolError("empty declaration payload")

    # Strict request form: exactly the 3-digit declaration code.
    if len(compact) == 3 and compact.isdigit():
        return DeclarationRequest(code_str=compact)

    # Completion form with a 3-digit prefix.
    if len(compact) >= 3 and compact[:3].isdigit():
        rest = compact[3:].strip()
        if rest:
            return DeclarationRequest(code_str=compact[:3], completed_text=rest)
        return DeclarationRequest(code_str=compact[:3])

    # Completion form without the code prefix; preserve the text so the
    # dashboard can still parse employee / plan / package values.
    return DeclarationRequest(code_str="", completed_text=compact)


# -----------------------------
# Human-readable status helpers
# -----------------------------
def interpret_status(event: StatusEvent | CompleteStatusEvent | FullStatusI40Event) -> Dict[str, Any]:
    stat1 = event.stat1
    stat2 = event.stat2
    stat3 = event.stat3

    running = (stat1 & 0xF8) == 0x00
    weft_stop = (stat1 & 0xF8) == 0xC0
    warp_stop = (stat1 & 0xF8) == 0xA0
    other_stop = (stat1 & 0xF8) == 0x90
    kpick_toggle = bool(stat1 & 0x04)

    result: Dict[str, Any] = {
        "raw": {"stat1": stat1, "stat2": stat2, "stat3": stat3},
        "running": running,
        "kpick_toggle": kpick_toggle,
        "category": "unknown",
        "detail": None,
    }

    if running:
        result["category"] = "running"
        return result

    if weft_stop:
        result["category"] = "weft_stop"
        mapping = {0x80: "weft_before_exchange", 0x40: "double_weft", 0x20: "weft_after_exchange"}
        result["detail"] = mapping.get(stat2, "unknown_weft_stop")
        result["weft_no"] = stat3
        return result

    if warp_stop:
        result["category"] = "warp_stop"
        mapping = {0x80: "warp_1", 0x40: "warp_2"}
        result["detail"] = mapping.get(stat2, "unknown_warp_stop")
        return result

    if other_stop:
        result["category"] = "other_stop"
        mapping = {
            0x80: "operator_stop",
            0x40: "auxiliary_halt",
            0x20: "mechanical",
            0x10: "empty_prewinder",
            0x08: "preselection",
            0x04: "beam_end",
        }
        result["detail"] = mapping.get(stat2, "unknown_other_stop")
        if stat2 == 0x10:
            result["weft_no"] = stat3
        return result

    result["category"] = "generic_stop"
    return result


# -----------------------------
# SQLite logger
# -----------------------------
class SQLiteStore:
    def __init__(self, db_path: str, logger: logging.Logger) -> None:
        self.db_path = db_path
        self.log = logger
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS raw_frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                ts_iso TEXT NOT NULL,
                peer_ip TEXT NOT NULL,
                peer_port INTEGER NOT NULL,
                transport TEXT NOT NULL,
                direction TEXT NOT NULL,
                cmd INTEGER NOT NULL,
                length INTEGER NOT NULL,
                hex TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_raw_frames_ts ON raw_frames(ts DESC);
            CREATE INDEX IF NOT EXISTS idx_raw_frames_ip ON raw_frames(peer_ip, ts DESC);

            CREATE TABLE IF NOT EXISTS loom_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                ts_iso TEXT NOT NULL,
                peer_ip TEXT NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT NOT NULL,
                payload_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_loom_events_ts ON loom_events(ts DESC);
            CREATE INDEX IF NOT EXISTS idx_loom_events_ip ON loom_events(peer_ip, ts DESC);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def log_frame(self, frame: Frame) -> None:
        self.conn.execute(
            """
            INSERT INTO raw_frames (ts, ts_iso, peer_ip, peer_port, transport, direction, cmd, length, hex)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                frame.recv_ts,
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(frame.recv_ts)),
                frame.peer_ip,
                frame.peer_port,
                frame.transport,
                frame.direction,
                frame.cmd,
                frame.length,
                frame.as_hex(),
            ),
        )
        self.conn.commit()

    def log_event(self, peer_ip: str, event_type: str, source: str, payload: Dict[str, Any]) -> None:
        ts = time.time()
        self.conn.execute(
            """
            INSERT INTO loom_events (ts, ts_iso, peer_ip, event_type, source, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (ts, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts)), peer_ip, event_type, source, json.dumps(payload)),
        )
        self.conn.commit()

    def recent_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, ts_iso, peer_ip, event_type, source, payload_json FROM loom_events ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "id": row["id"],
                "ts_iso": row["ts_iso"],
                "peer_ip": row["peer_ip"],
                "event_type": row["event_type"],
                "source": row["source"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def recent_frames(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT id, ts_iso, peer_ip, peer_port, transport, direction, cmd, length, hex FROM raw_frames ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


# -----------------------------
# Registry / declarations
# -----------------------------
class LoomRegistry:
    def __init__(self, loom_configs: List[Dict[str, Any]]) -> None:
        self.by_ip: Dict[str, LoomRuntimeState] = {}
        self.config_by_ip: Dict[str, Dict[str, Any]] = {}
        for entry in loom_configs:
            ip = entry["ip"]
            state = LoomRuntimeState(
                name=entry.get("name", ip),
                ip=ip,
                ts_port=int(entry.get("ts_port", DEFAULT_TS_PORT)),
                supports_qt5_full_status=bool(entry.get("supports_qt5_full_status", False)),
            )
            self.by_ip[ip] = state
            self.config_by_ip[ip] = entry

    def get_or_create_unknown(self, ip: str) -> LoomRuntimeState:
        if ip not in self.by_ip:
            self.by_ip[ip] = LoomRuntimeState(name=f"unknown-{ip}", ip=ip)
            self.config_by_ip[ip] = {"name": f"unknown-{ip}", "ip": ip, "declarations": {}}
        return self.by_ip[ip]

    def mark_tc_connected(self, ip: str, port: int) -> LoomRuntimeState:
        state = self.get_or_create_unknown(ip)
        state.tcp_connected = True
        state.current_peer_port = port
        state.last_tc_rx_ts = time.time()
        return state

    def mark_tc_disconnected(self, ip: str) -> None:
        state = self.get_or_create_unknown(ip)
        state.tcp_connected = False
        state.current_peer_port = None

    def mark_tc_rx(self, ip: str) -> None:
        self.get_or_create_unknown(ip).last_tc_rx_ts = time.time()

    def mark_ts_poll(self, ip: str) -> None:
        self.get_or_create_unknown(ip).last_ts_poll_ts = time.time()

    def set_last_status(self, ip: str, event: StatusEvent | CompleteStatusEvent | FullStatusI40Event) -> None:
        state = self.get_or_create_unknown(ip)
        state.last_status = {
            "event": asdict(event),
            "interpreted": interpret_status(event),
            "updated_at": time.time(),
            "updated_at_iso": now_iso(),
        }

    def declaration_reply_for(self, ip: str, code_str: str) -> Optional[str]:
        return self.config_by_ip.get(ip, {}).get("declarations", {}).get(code_str)

    def all_known_looms(self) -> List[LoomRuntimeState]:
        return list(self.by_ip.values())

    def snapshot(self) -> Dict[str, Any]:
        return {
            ip: {
                "name": state.name,
                "ip": state.ip,
                "ts_port": state.ts_port,
                "tcp_connected": state.tcp_connected,
                "current_peer_port": state.current_peer_port,
                "last_tc_rx_ts": state.last_tc_rx_ts,
                "last_ts_poll_ts": state.last_ts_poll_ts,
                "last_status": state.last_status,
                "supports_qt5_full_status": state.supports_qt5_full_status,
            }
            for ip, state in sorted(self.by_ip.items())
        }


# -----------------------------
# Event sink
# -----------------------------
class EventSink:
    def __init__(self, store: Optional[SQLiteStore], logger: logging.Logger) -> None:
        self.store = store
        self.log = logger

    def log_frame(self, frame: Frame) -> None:
        if self.store:
            self.store.log_frame(frame)

    def log_event(self, peer_ip: str, event_type: str, source: str, payload: Dict[str, Any]) -> None:
        if self.store:
            self.store.log_event(peer_ip, event_type, source, payload)


# -----------------------------
# TS client
# -----------------------------
class TsClient:
    def __init__(self, connect_timeout: float, reply_timeout: float, logger: logging.Logger, sink: EventSink) -> None:
        self.connect_timeout = connect_timeout
        self.reply_timeout = reply_timeout
        self.log = logger
        self.sink = sink

    async def transact(self, loom_ip: str, port: int, request: bytes) -> Frame:
        self.log.debug("TS connect %s:%s req=%s", loom_ip, port, request.hex().upper())
        reader, writer = await asyncio.wait_for(asyncio.open_connection(loom_ip, port), timeout=self.connect_timeout)
        try:
            tx_frame = Frame(
                cmd=request[0],
                length=request[1] if len(request) > 1 else 0,
                payload=request[2:],
                transport="tcp",
                direction="tx",
                peer_ip=loom_ip,
                peer_port=port,
            )
            self.sink.log_frame(tx_frame)
            writer.write(request)
            await writer.drain()

            parser = LoomStreamParser()
            deadline = time.monotonic() + self.reply_timeout
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"TS reply timeout from {loom_ip}:{port}")
                chunk = await asyncio.wait_for(reader.read(4096), timeout=remaining)
                if not chunk:
                    raise ConnectionError(f"loom closed before complete TS reply: {loom_ip}:{port}")
                frames = parser.feed(chunk)
                if frames:
                    frame = frames[0]
                    frame.transport = "tcp"
                    frame.direction = "rx"
                    frame.peer_ip = loom_ip
                    frame.peer_port = port
                    self.sink.log_frame(frame)
                    return frame
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass


# -----------------------------
# TC TCP server
# -----------------------------
class TcTcpServer:
    def __init__(
        self,
        registry: LoomRegistry,
        declaration_resolver: Callable[[str, str], Optional[str]],
        logger: logging.Logger,
        sink: EventSink,
    ) -> None:
        self.registry = registry
        self.resolve_declaration = declaration_resolver
        self.log = logger
        self.sink = sink

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        peer_ip, peer_port = (peer[0], peer[1]) if peer else ("unknown", 0)
        self.registry.mark_tc_connected(peer_ip, peer_port)
        self.log.info("TC TCP connected from %s:%s", peer_ip, peer_port)
        parser = LoomStreamParser()

        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    self.log.info("TC TCP EOF from %s:%s", peer_ip, peer_port)
                    break
                self.registry.mark_tc_rx(peer_ip)
                for frame in parser.feed(chunk):
                    frame.transport = "tcp"
                    frame.direction = "rx"
                    frame.peer_ip = peer_ip
                    frame.peer_port = peer_port
                    self.sink.log_frame(frame)
                    await self._handle_frame(frame, writer)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.log.exception("TC TCP session error from %s:%s", peer_ip, peer_port)
        finally:
            self.registry.mark_tc_disconnected(peer_ip)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            self.log.info("TC TCP disconnected from %s:%s", peer_ip, peer_port)

    async def _handle_frame(self, frame: Frame, writer: asyncio.StreamWriter) -> None:
        ip = frame.peer_ip
        self.log.debug("TC RX %s cmd=0x%02X len=%d raw=%s", ip, frame.cmd, frame.length, frame.as_hex())

        if frame.cmd == CMD_STATUS:
            event = decode_status(frame.payload, source="TC_PUSH")
            self.registry.set_last_status(ip, event)
            self.sink.log_event(ip, "status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
            self.log.info("TC status from %s: %s", ip, interpret_status(event))
            return

        if frame.cmd == CMD_COMPLETE_STATUS:
            event = decode_complete_status(frame.payload, source="TC_PUSH")
            self.registry.set_last_status(ip, event)
            self.sink.log_event(ip, "complete_status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
            self.log.info("TC complete-status from %s: speed=%s picks=%s status=%s", ip, event.speed_rpm, event.total_picks, interpret_status(event))
            return

        if frame.cmd == CMD_DECLARATION:
            decl = decode_declaration_request(frame)
            if decl.completed_text is None:
                reply = self.resolve_declaration(ip, decl.code_str)
                payload = {"code": decl.code_str, "reply": reply, "interactive": bool(reply and "_" in reply)}
                self.sink.log_event(ip, "declaration_request", "TC_PUSH", payload)
                self.log.info("TC declaration request from %s: code=%s reply=%r", ip, decl.code_str, reply)
                if reply is not None:
                    raw = encode_declaration_reply(reply)
                    tx_frame = Frame(
                        cmd=raw[0],
                        length=raw[1],
                        payload=raw[2:],
                        transport="tcp",
                        direction="tx",
                        peer_ip=ip,
                        peer_port=frame.peer_port,
                    )
                    self.sink.log_frame(tx_frame)
                    writer.write(raw)
                    await writer.drain()
                return
            payload = {"code": decl.code_str, "completed_text": decl.completed_text}
            self.sink.log_event(ip, "declaration_completion", "TC_PUSH", payload)
            self.log.info("TC declaration completion from %s: code=%s text=%r", ip, decl.code_str, decl.completed_text)
            return

        self.log.info("Unhandled TC frame from %s: cmd=0x%02X len=%d", ip, frame.cmd, frame.length)


# -----------------------------
# UDP listener (optional)
# -----------------------------
class TcUdpProtocol(asyncio.DatagramProtocol):
    def __init__(self, registry: LoomRegistry, logger: logging.Logger, sink: EventSink) -> None:
        self.registry = registry
        self.log = logger
        self.sink = sink
        self.parsers: Dict[Tuple[str, int], LoomStreamParser] = {}

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        ip, port = addr
        self.registry.mark_tc_rx(ip)
        parser = self.parsers.setdefault(addr, LoomStreamParser())
        for frame in parser.feed(data):
            frame.transport = "udp"
            frame.direction = "rx"
            frame.peer_ip = ip
            frame.peer_port = port
            self.sink.log_frame(frame)
            self.log.debug("TC UDP RX %s:%s cmd=0x%02X len=%d raw=%s", ip, port, frame.cmd, frame.length, frame.as_hex())

            try:
                if frame.cmd == CMD_STATUS:
                    event = decode_status(frame.payload, source="UDP_PUSH")
                    self.registry.set_last_status(ip, event)
                    self.sink.log_event(ip, "status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
                elif frame.cmd == CMD_COMPLETE_STATUS:
                    event = decode_complete_status(frame.payload, source="UDP_PUSH")
                    self.registry.set_last_status(ip, event)
                    self.sink.log_event(ip, "complete_status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
                elif frame.cmd == CMD_DECLARATION:
                    decl = decode_declaration_request(frame)
                    payload = {"code": decl.code_str, "completed_text": decl.completed_text}
                    self.sink.log_event(ip, "declaration", "UDP_PUSH", payload)
                else:
                    self.log.info("Unhandled UDP frame from %s:%s cmd=0x%02X len=%d", ip, port, frame.cmd, frame.length)
            except Exception:
                self.log.exception("Failed to decode UDP frame from %s:%s", ip, port)


# -----------------------------
# Minimal HTTP dashboard
# -----------------------------
class HttpDashboardServer:
    def __init__(self, registry: LoomRegistry, sink: EventSink, store: Optional[SQLiteStore], logger: logging.Logger) -> None:
        self.registry = registry
        self.sink = sink
        self.store = store
        self.log = logger

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            request_line = head.decode("iso-8859-1", errors="replace").split("\r\n", 1)[0]
            method, target, _ = request_line.split(" ", 2)
            parsed = urlparse(target)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if method != "GET":
                await self._send_response(writer, 405, "text/plain; charset=utf-8", b"Method Not Allowed")
                return

            if path == "/health":
                await self._send_response(writer, 200, "text/plain; charset=utf-8", b"OK\n")
                return

            if path == "/api/looms":
                payload = json.dumps(self.registry.snapshot(), indent=2).encode("utf-8")
                await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                return

            if path == "/api/events":
                limit = min(max(int(qs.get("limit", ["100"])[0]), 1), 1000)
                payload = json.dumps(self.store.recent_events(limit) if self.store else [], indent=2).encode("utf-8")
                await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                return

            if path == "/api/frames":
                limit = min(max(int(qs.get("limit", ["100"])[0]), 1), 1000)
                payload = json.dumps(self.store.recent_frames(limit) if self.store else [], indent=2).encode("utf-8")
                await self._send_response(writer, 200, "application/json; charset=utf-8", payload)
                return

            html_bytes = self._render_html().encode("utf-8")
            await self._send_response(writer, 200, "text/html; charset=utf-8", html_bytes)
        except asyncio.IncompleteReadError:
            pass
        except Exception:
            self.log.exception("HTTP dashboard request failed")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_response(self, writer: asyncio.StreamWriter, status: int, content_type: str, body: bytes) -> None:
        reason = {
            200: "OK",
            404: "Not Found",
            405: "Method Not Allowed",
        }.get(status, "OK")
        headers = [
            f"HTTP/1.1 {status} {reason}",
            f"Content-Type: {content_type}",
            f"Content-Length: {len(body)}",
            "Connection: close",
            "Cache-Control: no-store",
            "",
            "",
        ]
        writer.write("\r\n".join(headers).encode("ascii") + body)
        await writer.drain()

    def _render_html(self) -> str:
        rows = []
        snapshot = self.registry.snapshot()
        for ip, item in snapshot.items():
            last_status = item.get("last_status")
            summary = html.escape(json.dumps(last_status, indent=2)) if last_status else "&mdash;"
            rows.append(
                f"<tr><td>{html.escape(item['name'])}</td><td>{html.escape(ip)}</td>"
                f"<td>{'yes' if item['tcp_connected'] else 'no'}</td>"
                f"<td>{item['ts_port']}</td><td><pre>{summary}</pre></td></tr>"
            )
        recent_events = self.store.recent_events(20) if self.store else []
        events_html = "".join(
            f"<tr><td>{html.escape(ev['ts_iso'])}</td><td>{html.escape(ev['peer_ip'])}</td>"
            f"<td>{html.escape(ev['event_type'])}</td><td><pre>{html.escape(json.dumps(ev['payload'], indent=2))}</pre></td></tr>"
            for ev in recent_events
        )
        return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Loom Host Dashboard</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 1.5rem; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
    th, td {{ border: 1px solid #ccc; padding: 0.5rem; vertical-align: top; }}
    th {{ background: #f5f5f5; text-align: left; }}
    pre {{ white-space: pre-wrap; word-break: break-word; margin: 0; }}
    .muted {{ color: #666; font-size: 0.9rem; }}
  </style>
</head>
<body>
  <h1>Loom Host Dashboard</h1>
  <div class="muted">Updated {html.escape(now_iso())}</div>
  <h2>Known Looms</h2>
  <table>
    <thead><tr><th>Name</th><th>IP</th><th>TC Connected</th><th>TS Port</th><th>Last Status</th></tr></thead>
    <tbody>{''.join(rows) if rows else '<tr><td colspan="5">No looms configured</td></tr>'}</tbody>
  </table>
  <h2>Recent Events</h2>
  <table>
    <thead><tr><th>Time</th><th>Peer IP</th><th>Event</th><th>Payload</th></tr></thead>
    <tbody>{events_html if events_html else '<tr><td colspan="4">No events recorded</td></tr>'}</tbody>
  </table>
</body>
</html>
"""


# -----------------------------
# Polling manager
# -----------------------------
class PollingManager:
    def __init__(self, registry: LoomRegistry, ts_client: TsClient, sink: EventSink, logger: logging.Logger) -> None:
        self.registry = registry
        self.ts_client = ts_client
        self.sink = sink
        self.log = logger
        self._tasks: List[asyncio.Task[Any]] = []
        self._stop = False

    def start(self) -> None:
        for state in self.registry.all_known_looms():
            cfg = self.registry.config_by_ip[state.ip]
            enabled = bool(cfg.get("enabled", True))
            if not enabled:
                self.log.info("Skipping background polling for disabled loom %s", state.ip)
                continue
            status_every = float(cfg.get("poll_status_every_seconds", 5))
            full_every = float(cfg.get("poll_full_status_every_seconds", 10))
            supports_qt5 = bool(cfg.get("supports_qt5_full_status", False))
            self._tasks.append(asyncio.create_task(self._poll_status_loop(state.ip, state.ts_port, status_every)))
            if supports_qt5:
                if full_every < 6.0:
                    self.log.warning("Configured full-status poll interval for %s is %.3fs; enforcing minimum 6.0s", state.ip, full_every)
                    full_every = 6.0
                self._tasks.append(asyncio.create_task(self._poll_full_status_loop(state.ip, state.ts_port, full_every)))

    async def stop(self) -> None:
        self._stop = True
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def _poll_status_loop(self, ip: str, port: int, every_s: float) -> None:
        while not self._stop:
            try:
                frame = await self.ts_client.transact(ip, port, encode_status_request())
                event = decode_status(frame.payload, source="TS_REPLY")
                self.registry.mark_ts_poll(ip)
                self.registry.set_last_status(ip, event)
                self.sink.log_event(ip, "status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
                self.log.info("TS status from %s: %s", ip, interpret_status(event))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("TS status poll failed for %s:%s: %s", ip, port, exc)
            await asyncio.sleep(every_s)

    async def _poll_full_status_loop(self, ip: str, port: int, every_s: float) -> None:
        while not self._stop:
            try:
                frame = await self.ts_client.transact(ip, port, encode_full_status_request())
                event = decode_full_status(frame.payload, source="TS_REPLY")
                self.registry.mark_ts_poll(ip)
                self.registry.set_last_status(ip, event)
                self.sink.log_event(ip, "full_status", event.source, {"event": asdict(event), "interpreted": interpret_status(event)})
                self.log.info(
                    "TS full-status from %s: speed=%s picks=%s eff=%.2f%%",
                    ip,
                    event.speed_rpm,
                    event.total_picks,
                    event.efficiency_x100 / 100,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.warning("TS full-status poll failed for %s:%s: %s", ip, port, exc)
            await asyncio.sleep(every_s)


# -----------------------------
# Application bootstrap
# -----------------------------
class LoomHostApp:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.log = logging.getLogger("loom_host")
        self.registry = LoomRegistry(config.get("looms", []))
        self.store: Optional[SQLiteStore] = None
        if config.get("sqlite_db_path"):
            self.store = SQLiteStore(config["sqlite_db_path"], self.log)
        self.sink = EventSink(self.store, self.log)
        self.ts_client = TsClient(
            connect_timeout=float(config.get("ts_connect_timeout_seconds", 3.0)),
            reply_timeout=float(config.get("ts_reply_timeout_seconds", 2.0)),
            logger=self.log,
            sink=self.sink,
        )
        self.tc_tcp_server = TcTcpServer(self.registry, self.registry.declaration_reply_for, self.log, self.sink)
        self.http_dashboard = HttpDashboardServer(self.registry, self.sink, self.store, self.log)
        self.polling = PollingManager(self.registry, self.ts_client, self.sink, self.log)
        self._tcp_server: Optional[asyncio.AbstractServer] = None
        self._http_server: Optional[asyncio.AbstractServer] = None
        self._udp_transport: Optional[asyncio.DatagramTransport] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        listen_host = self.config.get("listen_host", "0.0.0.0")
        host_port = int(self.config.get("host_port", DEFAULT_HOST_PORT))
        self._tcp_server = await asyncio.start_server(self.tc_tcp_server.handle_client, listen_host, host_port)
        self.log.info("TC TCP server listening on %s:%s", listen_host, host_port)

        if self.config.get("enable_udp_listener", True):
            loop = asyncio.get_running_loop()
            transport, _protocol = await loop.create_datagram_endpoint(
                lambda: TcUdpProtocol(self.registry, self.log, self.sink),
                local_addr=(listen_host, host_port),
            )
            self._udp_transport = transport
            self.log.info("TC UDP listener on %s:%s", listen_host, host_port)

        if self.config.get("enable_http_dashboard", True):
            http_host = self.config.get("http_host", "127.0.0.1")
            http_port = int(self.config.get("http_port", DEFAULT_HTTP_PORT))
            self._http_server = await asyncio.start_server(self.http_dashboard.handle_client, http_host, http_port)
            self.log.info("HTTP dashboard on http://%s:%s", http_host, http_port)

        self.polling.start()

    async def stop(self) -> None:
        await self.polling.stop()
        if self._udp_transport is not None:
            self._udp_transport.close()
            self._udp_transport = None
        if self._tcp_server is not None:
            self._tcp_server.close()
            await self._tcp_server.wait_closed()
            self._tcp_server = None
        if self._http_server is not None:
            self._http_server.close()
            await self._http_server.wait_closed()
            self._http_server = None
        if self.store:
            self.store.close()
            self.store = None

    def request_stop(self) -> None:
        self._stop_event.set()

    async def run_until_stopped(self) -> None:
        await self.start()
        await self._stop_event.wait()

    async def print_snapshot_loop(self, every_s: float) -> None:
        while True:
            await asyncio.sleep(every_s)
            print(json.dumps(self.registry.snapshot(), indent=2))


# -----------------------------
# Config / CLI
# -----------------------------
def default_config() -> Dict[str, Any]:
    return {
        "listen_host": "0.0.0.0",
        "host_port": DEFAULT_HOST_PORT,
        "enable_udp_listener": True,
        "ts_connect_timeout_seconds": 3.0,
        "ts_reply_timeout_seconds": 2.0,
        "enable_http_dashboard": True,
        "http_host": "127.0.0.1",
        "http_port": DEFAULT_HTTP_PORT,
        "sqlite_db_path": "loom_events.db",
        "looms": [],
    }


def load_config(path: Optional[str]) -> Dict[str, Any]:
    cfg = default_config()
    if path:
        with open(path, "r", encoding="utf-8") as f:
            user_cfg = json.load(f)
        cfg.update(user_cfg)
    return cfg


async def async_main(args: argparse.Namespace) -> int:
    app = LoomHostApp(load_config(args.config))
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


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Enhanced SMIT loom host")
    p.add_argument("--config", default=None, help="Path to JSON config file")
    p.add_argument("--log-level", default="INFO", help="Logging level")
    p.add_argument("--snapshot-interval", type=float, default=0.0, help="Print registry snapshot every N seconds")
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        return asyncio.run(async_main(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
