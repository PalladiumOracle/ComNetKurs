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

                try:
                    frames = decoder.feed(data)
                except ValueError as exc:
                    logger.warning("Ошибка декодирования от %s: %s", peer, exc)
                    break

                for frame in frames:
                    logger.info(
                        "Получен кадр: type=%d, request_id=%d, payload=%d байт",
                        frame.header.type, frame.header.request_id,
                        frame.header.payload_length,
                    )
                    try:
                        response = await on_frame(frame)
                    except Exception as exc:
                        logger.exception(
                            "Ошибка в обработчике кадра от %s: %s", peer, exc
                        )
                        continue  
                    if response is not None:
                        writer.write(response.encode())
                        await writer.drain()
                        logger.info(
                            "Отправлен кадр: type=%d, request_id=%d, payload=%d байт",
                            response.header.type, response.header.request_id,
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
    expected_request_id: int | None = None,
) -> Frame | None:
    logger.info("Подключение к %s:%d", host, port)
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port), timeout=connection_timeout
    )
    decoder = StreamDecoder(max_frame_payload=max_frame_payload)
    try:
        writer.write(frame.encode())
        await writer.drain()
        logger.info(
            "Отправлен кадр: type=%d, request_id=%d, payload=%d байт",
            frame.header.type, frame.header.request_id,
            frame.header.payload_length,
        )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + connection_timeout

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning(
                    "Тайм-аут ожидания правильного ответа от %s:%d", host, port
                )
                return None

            try:
                data = await asyncio.wait_for(
                    reader.read(_READ_CHUNK_SIZE), timeout=remaining,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Тайм-аут ожидания правильного ответа от %s:%d", host, port
                )
                return None

            if not data:
                logger.warning("Соединение закрыто до получения правильного ответа")
                return None

            try:
                frames = decoder.feed(data)
            except ValueError as exc:
                logger.warning("Ошибка декодирования ответа: %s", exc)
                continue

            for resp in frames:
                if expected_request_id is None or resp.header.request_id == expected_request_id:
                    logger.info(
                        "Принят ответ: request_id=%d, type=%d",
                        resp.header.request_id, resp.header.type,
                    )
                    return resp
                else:
                    logger.warning(
                        "Ответ отклонён: request_id=%d, ожидался %d",
                        resp.header.request_id, expected_request_id,
                    )
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
