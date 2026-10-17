from __future__ import annotations
import asyncio
import logging
from typing import Optional
from p2p.config import NodeConfig
from p2p.transport.connection import start_server, connect_and_send

logger = logging.getLogger(__name__)


class Node:
    def __init__(self, config: NodeConfig) -> None:
        self.config = config
        self._server: Optional[asyncio.AbstractServer] = None

    async def start(self) -> None:
        self._server = await start_server(
            host=self.config.host,
            port=self.config.port,
            on_message=self._on_message,
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

    async def _on_message(self, data: bytes) -> bytes | None:
        text = data.decode("utf-8", errors="replace")
        logger.info("Узел %s получил: %r", self.config.node_id, text)
        response_text = f"ACK from {self.config.node_id}: Успешное соединение"
        return response_text.encode("utf-8")

    async def send_to(
        self, host: str, port: int, message: str,
        *, expect_response: bool = True,
    ) -> Optional[bytes]:
        return await connect_and_send(
            host=host, port=port,
            message=message.encode("utf-8"),
            connection_timeout=self.config.connection_timeout,
            expect_response=expect_response,
        )
