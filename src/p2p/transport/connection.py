from __future__ import annotations
import asyncio
import logging
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)
MessageHandler = Callable[[bytes], Awaitable[bytes | None]]

async def start_server(
    host: str,
    port: int,
    on_message: MessageHandler,
    *,
    connection_timeout: float = 30.0,
) -> asyncio.AbstractServer:
    
    async def handle_client(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        logger.info("Соединение установлено: %s", peer)

        try:
            while True:
                try:
                    data = await asyncio.wait_for(
                        reader.read(4096), timeout=connection_timeout
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Тайм-аут чтения от %s - закрытие соединения", peer
                    )
                    break

                if not data:
                    logger.info("Соединение закрыто удалённой стороной: %s", peer)
                    break

                logger.info("Получено %d байт от %s", len(data), peer)

                response = await on_message(data)
                if response is not None:
                    writer.write(response)
                    await writer.drain()
                    logger.info("Отправлено %d байт в %s", len(response), peer)
        except Exception as exc:
            logger.exception("Ошибка при обработке клиента %s: %s", peer, exc)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("Соединение с %s закрыто", peer)

    server = await asyncio.start_server(handle_client, host, port)
    logger.info("Сервер запущен на %s:%d", host, port)
    return server


async def connect_and_send(
    host: str,
    port: int,
    message: bytes,
    *,
    connection_timeout: float = 30.0,
    expect_response: bool = True,
) -> bytes | None:
    logger.info("Подключение к %s:%d", host, port)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=connection_timeout
    )
    try:
        writer.write(message)
        await writer.drain()
        logger.info("Отправлено %d байт", len(message))

        if not expect_response:
            return None

        try:
            response = await asyncio.wait_for(
                reader.read(4096), timeout=connection_timeout
            )
        except asyncio.TimeoutError:
            logger.warning("Тайм-аут ожидания ответа от %s:%d", host, port)
            return None

        logger.info("Получено %d байт ответа", len(response))
        return response
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
