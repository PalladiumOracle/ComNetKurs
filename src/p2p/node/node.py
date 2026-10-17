from __future__ import annotations
import asyncio
import logging
from typing import Optional
from p2p.config import NodeConfig
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    FRAME_TYPE_HELLO_ACK,
    Frame,
    FrameHeader,
    PROTOCOL_VERSION,
)
from p2p.transport.connection import connect_and_send, start_server

logger = logging.getLogger(__name__)

class Node:
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self._server: Optional[asyncio.AbstractServer] = None
        self._next_request_id: int = 1

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
        text = frame.payload.decode("utf-8", errors="replace")
        logger.info("Узел %s получил: %r", self.config.node_id, text)
        response_text = f"ACK from {self.config.node_id}: Соединение успешно"
        response_payload = response_text.encode("utf-8")
        response_header = FrameHeader(
            version=PROTOCOL_VERSION,
            type=FRAME_TYPE_HELLO_ACK,
            flags=0,
            request_id=frame.header.request_id,   
            payload_length=len(response_payload),
        )
        return Frame(header=response_header, payload=response_payload)

    async def send_to(
        self, host: str, port: int, message: str,
    ) -> Optional[Frame]:
        payload = message.encode("utf-8")
        header = FrameHeader(
            version=PROTOCOL_VERSION,
            type=FRAME_TYPE_DATA,
            flags=0,
            request_id=self.next_request_id(),
            payload_length=len(payload),
        )
        frame = Frame(header=header, payload=payload)
        return await connect_and_send(
            host=host, port=port,
            frame=frame,
            max_frame_payload=self.config.max_frame_payload,
            connection_timeout=self.config.connection_timeout,
        )
