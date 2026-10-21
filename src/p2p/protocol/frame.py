from __future__ import annotations
import struct
from dataclasses import dataclass

HEADER_SIZE: int = 24
PROTOCOL_VERSION: int = 1
FRAME_TYPE_HELLO: int = 1
FRAME_TYPE_HELLO_ACK: int = 2
FRAME_TYPE_BYE: int = 3
FRAME_TYPE_BYE_ACK: int = 4
FRAME_TYPE_DATA: int = 16
FRAME_TYPE_ERROR: int = 255
_HEADER_STRUCT = struct.Struct(">BBHQIQ")

assert _HEADER_STRUCT.size == HEADER_SIZE, (
    f"Ожидался заголовок {HEADER_SIZE} байт, получено {_HEADER_STRUCT.size}"
)


@dataclass(frozen=True)
class FrameHeader:
    version: int
    type: int
    flags: int
    request_id: int
    payload_length: int
    reserved: int = 0

    def encode(self) -> bytes:
        return _HEADER_STRUCT.pack(
            self.version,
            self.type,
            self.flags,
            self.request_id,
            self.payload_length,
            self.reserved,
        )

    @classmethod
    def decode(cls, data: bytes) -> FrameHeader:
        if len(data) != HEADER_SIZE:
            raise ValueError(
                f"Заголовок должен быть {HEADER_SIZE} байт, "
                f"получено {len(data)}"
            )
        version, type_, flags, request_id, payload_length, reserved = (
            _HEADER_STRUCT.unpack(data)
        )
        return cls(
            version=version,
            type=type_,
            flags=flags,
            request_id=request_id,
            payload_length=payload_length,
            reserved=reserved,
        )

    def validate(self, max_payload: int) -> None:
        if self.version != PROTOCOL_VERSION:
            raise ValueError(
                f"Неподдерживаемая версия протокола: {self.version}"
            )
        if self.payload_length > max_payload:
            raise ValueError(
                f"payload_length={self.payload_length} превышает "
                f"max_frame_payload={max_payload}"
            )


@dataclass(frozen=True)
class Frame:
    header: FrameHeader
    payload: bytes

    def encode(self) -> bytes:
        if len(self.payload) != self.header.payload_length:
            raise ValueError(
                f"payload_length={self.header.payload_length}, "
                f"фактическая длина payload={len(self.payload)}"
            )
        return self.header.encode() + self.payload

    @classmethod
    def decode(cls, data: bytes) -> Frame:
        if len(data) < HEADER_SIZE:
            raise ValueError(
                f"Данных меньше заголовка: {len(data)} < {HEADER_SIZE}"
            )
        header = FrameHeader.decode(data[:HEADER_SIZE])
        expected = HEADER_SIZE + header.payload_length
        if len(data) < expected:
            raise ValueError(
                f"Данных меньше, чем заявлено: {len(data)} < {expected}"
            )
        payload = data[HEADER_SIZE:expected]
        return cls(header=header, payload=payload)
