from __future__ import annotations
import asyncio
import logging
from typing import Awaitable, Callable
from p2p.protocol.frame import (
    HEADER_SIZE,
    Frame,
    FrameHeader,
)

logger = logging.getLogger(__name__)
FrameHandler = Callable[[Frame], Awaitable[Frame | None]]

async def start_server(
    host: str,
    port: int,
    on_frame: FrameHandler,
    *,
    max_frame_payload: int,
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
                    header_bytes = await asyncio.wait_for(
                        reader.readexactly(HEADER_SIZE),
                        timeout=connection_timeout,
                    )
                except asyncio.IncompleteReadError:
                    logger.info("Соединение закрыто (EOF): %s", peer)
                    break
                except asyncio.TimeoutError:
                    logger.warning(
                        "Тайм-аут чтения заголовка от %s — закрываем", peer
                    )
                    break
                try:
                    header = FrameHeader.decode(header_bytes)
                    header.validate(max_payload=max_frame_payload)
                except ValueError as exc:
                    logger.warning(
                        "Некорректный заголовок от %s: %s", peer, exc
                    )
                    break
                try:
                    payload = await asyncio.wait_for(
                        reader.readexactly(header.payload_length),
                        timeout=connection_timeout,
                    )
                except asyncio.IncompleteReadError:
                    logger.warning(
                        "Соединение оборвано внутри payload (%d байт) от %s",
                        header.payload_length, peer,
                    )
                    break
                except asyncio.TimeoutError:
                    logger.warning(
                        "Тайм-аут чтения payload от %s — закрываем", peer
                    )
                    break
                frame = Frame(header=header, payload=payload)
                logger.info(
                    "Получен кадр: type=%d, request_id=%d, payload=%d байт",
                    header.type, header.request_id, header.payload_length,
                )
                response = await on_frame(frame)
                if response is not None:
                    writer.write(response.encode())
                    await writer.drain()
                    logger.info(
                        "Отправлен кадр: type=%d, request_id=%d, payload=%d байт",
                        response.header.type,
                        response.header.request_id,
                        response.header.payload_length,
                    )
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
    frame: Frame,
    *,
    max_frame_payload: int,
    connection_timeout: float = 30.0,
) -> Frame | None:
    logger.info("Подключаемся к %s:%d", host, port)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=connection_timeout
    )
    try:
        writer.write(frame.encode())
        await writer.drain()
        logger.info(
            "Отправлен кадр: type=%d, request_id=%d, payload=%d байт",
            frame.header.type,
            frame.header.request_id,
            frame.header.payload_length,
        )
        try:
            header_bytes = await asyncio.wait_for(
                reader.readexactly(HEADER_SIZE),
                timeout=connection_timeout,
            )
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            logger.warning("Не удалось прочитать заголовок ответа")
            return None
        try:
            header = FrameHeader.decode(header_bytes)
            header.validate(max_payload=max_frame_payload)
        except ValueError as exc:
            logger.warning("Некорректный заголовок ответа: %s", exc)
            return None
        try:
            payload = await asyncio.wait_for(
                reader.readexactly(header.payload_length),
                timeout=connection_timeout,
            )
        except (asyncio.IncompleteReadError, asyncio.TimeoutError):
            logger.warning("Не удалось прочитать payload ответа")
            return None

        logger.info(
            "Получен ответ: type=%d, request_id=%d, payload=%d байт",
            header.type, header.request_id, header.payload_length,
        )
        return Frame(header=header, payload=payload)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
