from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional
from p2p.config import NodeConfig
from p2p.dht.routing import Contact, RoutingTable
from p2p.identity.identity import Identity, load_or_create_identity
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    FRAME_TYPE_FIND_NODE,
    FRAME_TYPE_FIND_NODE_RESPONSE,
    FRAME_TYPE_HELLO_ACK,
    FRAME_TYPE_PING,
    FRAME_TYPE_PONG,
    Frame,
    FrameHeader,
    PROTOCOL_VERSION,
)
from p2p.transport.connection import connect_and_send, start_server

logger = logging.getLogger(__name__)

class Node:
 
    _KNOWN_TYPES = {
        1, 2, 3, 4, 16,
        FRAME_TYPE_PING, FRAME_TYPE_PONG,
        FRAME_TYPE_FIND_NODE, FRAME_TYPE_FIND_NODE_RESPONSE,
    }

    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self._server: Optional[asyncio.AbstractServer] = None
        self._next_request_id: int = 1
        self.identity: Identity = load_or_create_identity(config.state_dir)
        self.routing = RoutingTable(local_id=self.identity.node_id, k=8)

        logger.info(
            "Узел %s: NodeID=%s", config.node_id, self.identity.node_id_hex
        )

    def next_request_id(self) -> int:
        rid = self._next_request_id
        self._next_request_id += 1
        return rid

    async def start(self) -> None:
        self._server = await start_server(
            host=self.config.host,
            port=self.config.port,
            on_frame=self._on_frame,
            max_frame_payload=self.config.max_frame_payload,
            connection_timeout=self.config.connection_timeout,
        )
        logger.info(
            "Узел %s запущен на %s:%d",
            self.config.node_id, self.config.host, self.config.port,
        )

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Узел %s остановлен", self.config.node_id)

    async def _on_frame(self, frame: Frame) -> Frame | None:
        if frame.header.type not in self._KNOWN_TYPES:
            logger.warning(
                "Узел %s: неизвестный тип %d",
                self.config.node_id, frame.header.type,
            )
            return None

        try:
            payload_str = frame.payload.decode("utf-8")
            payload = json.loads(payload_str) if payload_str else {}
        except Exception:
            payload = {}

        if frame.header.type == FRAME_TYPE_PING:
            return self._handle_ping(frame, payload)
        elif frame.header.type == FRAME_TYPE_FIND_NODE:
            return self._handle_find_node(frame, payload)
        elif frame.header.type == FRAME_TYPE_DATA:
            return self._handle_data(frame, payload)

        return None

    def _handle_ping(self, frame: Frame, payload: dict) -> Frame:
        logger.info(
            "Узел %s: получен PING от %s",
            self.config.node_id, frame.header.request_id,
        )
        resp_payload = {"type": "PONG"}
        return self._make_response(frame, FRAME_TYPE_PONG, resp_payload)

    def _handle_find_node(self, frame: Frame, payload: dict) -> Frame:
        target_hex = payload.get("target_id")
        limit = payload.get("limit", 8)
        logger.info(
            "Узел %s: получен FIND_NODE для %s",
            self.config.node_id, target_hex[:8] if target_hex else "None",
        )

        target_id = bytes.fromhex(target_hex) if target_hex else b"\x00" * 32
        closest = self.routing.closest(target_id, limit=limit)

        contacts = []
        for c in closest:
            contacts.append({
                "node_id": c.node_id_hex,
                "host": c.host,
                "port": c.port,
                "public_key": c.public_key.hex(),
            })

        resp_payload = {"contacts": contacts}
        return self._make_response(
            frame, FRAME_TYPE_FIND_NODE_RESPONSE, resp_payload
        )

    def _handle_data(self, frame: Frame, payload: dict) -> Frame:
        text = payload.get("text", "")
        logger.info(
            "Узел %s получил DATA: %r", self.config.node_id, text
        )
        resp_payload = {
            "text": f"ACK from {self.config.node_id}: {text}"
        }
        return self._make_response(
            frame, FRAME_TYPE_HELLO_ACK, resp_payload
        )

    def _make_response(
        self, req_frame: Frame, resp_type: int, payload_dict: dict
    ) -> Frame:
        payload_bytes = json.dumps(payload_dict).encode("utf-8")
        header = FrameHeader(
            version=PROTOCOL_VERSION,
            type=resp_type,
            flags=0,
            request_id=req_frame.header.request_id,
            payload_length=len(payload_bytes),
        )
        return Frame(header=header, payload=payload_bytes)

    async def ping(self, host: str, port: int) -> bool:
        """Отправляет PING и ждёт PONG."""
        rid = self.next_request_id()
        payload = json.dumps({"type": "PING"}).encode("utf-8")
        frame = Frame(
            header=FrameHeader(
                PROTOCOL_VERSION, FRAME_TYPE_PING, 0, rid, len(payload)
            ),
            payload=payload,
        )
        resp = await connect_and_send(
            host, port, frame,
            max_frame_payload=self.config.max_frame_payload,
            connection_timeout=self.config.connection_timeout,
            expected_request_id=rid,
        )
        if resp and resp.header.type == FRAME_TYPE_PONG:
            logger.info("Узел %s: PING успешен", self.config.node_id)
            return True
        return False

    async def find_node(
        self, host: str, port: int, target_id: bytes, limit: int = 8
    ) -> list[dict]:
        rid = self.next_request_id()
        payload = json.dumps({
            "target_id": target_id.hex(),
            "limit": limit,
        }).encode("utf-8")
        frame = Frame(
            header=FrameHeader(
                PROTOCOL_VERSION, FRAME_TYPE_FIND_NODE, 0, rid, len(payload)
            ),
            payload=payload,
        )
        resp = await connect_and_send(
            host, port, frame,
            max_frame_payload=self.config.max_frame_payload,
            connection_timeout=self.config.connection_timeout,
            expected_request_id=rid,
        )
        if resp and resp.header.type == FRAME_TYPE_FIND_NODE_RESPONSE:
            resp_payload = json.loads(resp.payload.decode("utf-8"))
            return resp_payload.get("contacts", [])
        return []

    async def send_data(self, host: str, port: int, text: str) -> Optional[str]:
        rid = self.next_request_id()
        payload = json.dumps({"text": text}).encode("utf-8")
        frame = Frame(
            header=FrameHeader(
                PROTOCOL_VERSION, FRAME_TYPE_DATA, 0, rid, len(payload)
            ),
            payload=payload,
        )
        resp = await connect_and_send(
            host, port, frame,
            max_frame_payload=self.config.max_frame_payload,
            connection_timeout=self.config.connection_timeout,
            expected_request_id=rid,
        )
        if resp:
            return json.loads(resp.payload.decode("utf-8")).get("text")
        return None

    def add_contact(self, contact: Contact) -> None:
        status, lru = self.routing.add_contact(contact)
        if status == "full":
            logger.warning(
                "Узел %s: bucket полон, LRU требует PING",
                self.config.node_id,
            )
