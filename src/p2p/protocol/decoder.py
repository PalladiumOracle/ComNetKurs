from __future__ import annotations
import logging
from p2p.protocol.frame import (
    HEADER_SIZE,
    Frame,
    FrameHeader,
)

logger = logging.getLogger(__name__)

class StreamDecoder:
    def __init__(self, max_frame_payload: int) -> None:
        self._buffer = bytearray()
        self._max = max_frame_payload

    @property
    def buffered_bytes(self) -> int:        
        return len(self._buffer)

    def feed(self, data: bytes) -> list[Frame]:
        self._buffer.extend(data)
        frames: list[Frame] = []

        while True:
            frame = self._try_extract()
            if frame is None:
                break
            frames.append(frame)

        return frames

    def _try_extract(self) -> Frame | None:
        if len(self._buffer) < HEADER_SIZE:
            return None
        header_bytes = bytes(self._buffer[:HEADER_SIZE])
        header = FrameHeader.decode(header_bytes)
        header.validate(max_payload=self._max)
        total = HEADER_SIZE + header.payload_length
        if len(self._buffer) < total:
            return None
        payload = bytes(self._buffer[HEADER_SIZE:total])
        del self._buffer[:total]

        logger.debug(
            "Извлечён кадр: type=%d, request_id=%d, payload=%d байт, "
            "осталось в буфере %d",
            header.type, header.request_id, header.payload_length,
            len(self._buffer),
        )
        return Frame(header=header, payload=payload)
