from __future__ import annotations
import pytest
from p2p.protocol.frame import (
    HEADER_SIZE,
    PROTOCOL_VERSION,
    FRAME_TYPE_DATA,
    Frame,
    FrameHeader,
)

class TestFrameHeader:
    def test_header_size(self) -> None:
        assert HEADER_SIZE == 24

    def test_roundtrip(self) -> None:
        original = FrameHeader(
            version=1, type=16, flags=0,
            request_id=12345, payload_length=42,
        )
        encoded = original.encode()
        assert len(encoded) == HEADER_SIZE
        decoded = FrameHeader.decode(encoded)
        assert decoded == original

    def test_known_bytes(self) -> None:
        """Сверка с эталонными байтами"""
        header = FrameHeader(
            version=1, type=1, flags=0,
            request_id=42, payload_length=16,
        )
        encoded = header.encode()
        expected = (
            b"\x01"                    #version
            b"\x01"                    #type
            b"\x00\x00"                #flags
            b"\x00\x00\x00\x00\x00\x00\x00\x2a"  #request_id = 42
            b"\x00\x00\x00\x10"        #payload_length = 16
            b"\x00\x00\x00\x00\x00\x00\x00\x00"  #reserved
        )
        assert encoded == expected
        assert len(expected) == HEADER_SIZE

    def test_decode_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="24 байт"):
            FrameHeader.decode(b"\x00" * 23)

    def test_validate_ok(self) -> None:
        header = FrameHeader(
            version=PROTOCOL_VERSION, type=FRAME_TYPE_DATA,
            flags=0, request_id=1, payload_length=100,
        )
        header.validate(max_payload=1024)  

    def test_validate_bad_version(self) -> None:
        header = FrameHeader(
            version=99, type=1, flags=0, request_id=1, payload_length=0,
        )
        with pytest.raises(ValueError, match="версия"):
            header.validate(max_payload=1024)

    def test_validate_bad_payload(self) -> None:
        header = FrameHeader(
            version=PROTOCOL_VERSION, type=1, flags=0,
            request_id=1, payload_length=5000,
        )
        with pytest.raises(ValueError, match="payload_length"):
            header.validate(max_payload=1024)

class TestFrame:
    def test_encode_decode(self) -> None:
        payload = b"Hello from node2"
        header = FrameHeader(
            version=PROTOCOL_VERSION, type=FRAME_TYPE_DATA,
            flags=0, request_id=1, payload_length=len(payload),
        )
        frame = Frame(header=header, payload=payload)
        encoded = frame.encode()
        assert len(encoded) == HEADER_SIZE + len(payload)
        decoded = Frame.decode(encoded)
        assert decoded.header == header
        assert decoded.payload == payload

    def test_encode_payload_mismatch(self) -> None:
        header = FrameHeader(
            version=1, type=16, flags=0, request_id=1, payload_length=100,
        )
        with pytest.raises(ValueError, match="длина payload"):
            Frame(header=header, payload=b"short").encode()

    def test_decode_too_short(self) -> None:
        with pytest.raises(ValueError, match="заголовка"):
            Frame.decode(b"\x00" * 10)

    def test_decode_payload_incomplete(self) -> None:
        header = FrameHeader(
            version=1, type=16, flags=0, request_id=1, payload_length=100,
        )
        data = header.encode() + b"\x00" * 50
        with pytest.raises(ValueError, match="меньше"):
            Frame.decode(data)
