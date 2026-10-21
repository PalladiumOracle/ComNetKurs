from __future__ import annotations
import asyncio
import logging
from typing import Awaitable, Callable
from p2p.protocol.decoder import StreamDecoder
from p2p.protocol.frame import Frame

logger = logging.getLogger(__name__)
FrameHandler = Callable[[Frame], Awaitable[Frame | None]]
_READ_CHUNK_SIZE = 4096


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
        decoder = StreamDecoder(max_frame_payload=max_frame_payload)

        try:
            while True:
                try:
                    data = await asyncio.wait_for(
                        reader.read(_READ_CHUNK_SIZE),
                        timeout=connection_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Тайм-аут чтения от %s", peer)
                    break

                if not data:
                    logger.info("Соединение закрыто (EOF): %s", peer)
                    break

                logger.debug("Прочитано %d байт от %s", len(data), peer)

                try:
                    frames = decoder.feed(data)
                except ValueError as exc:
                    logger.warning(
                        "Ошибка декодирования от %s: %s", peer, exc
                    )
                    break

                for frame in frames:
                    logger.info(
                        "Получен кадр: type=%d, request_id=%d, payload=%d байт",
                        frame.header.type,
                        frame.header.request_id,
                        frame.header.payload_length,
                    )
                    response = await on_frame(frame)
                    if response is not None:
                        writer.write(response.encode())
                        await writer.drain()
                        logger.info(
                            "Отправлен кадр: type=%d, request_id=%d, "
                            "payload=%d байт",
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
    decoder = StreamDecoder(max_frame_payload=max_frame_payload)
    try:
        writer.write(frame.encode())
        await writer.drain()
        logger.info(
            "Отправлен кадр: type=%d, request_id=%d, payload=%d байт",
            frame.header.type,
            frame.header.request_id,
            frame.header.payload_length,
        )
        deadline = asyncio.get_event_loop().time() + connection_timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                logger.warning("Тайм-аут ожидания ответа от %s:%d",
                               host, port)
                return None
            try:
                data = await asyncio.wait_for(
                    reader.read(_READ_CHUNK_SIZE), timeout=remaining,
                )
            except asyncio.TimeoutError:
                logger.warning("Тайм-аут ожидания ответа от %s:%d",
                               host, port)
                return None
            if not data:
                logger.warning("Соединение закрыто до получения ответа")
                return None
            try:
                frames = decoder.feed(data)
            except ValueError as exc:
                logger.warning("Ошибка декодирования ответа: %s", exc)
                return None
            if frames:
                if len(frames) > 1:
                    logger.info(
                        "В одном чанке пришло %d кадров; "
                        "возвращаем первый", len(frames),
                    )
                return frames[0]
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
