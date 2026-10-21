from __future__ import annotations
import asyncio
import pytest
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    FRAME_TYPE_HELLO_ACK,
    PROTOCOL_VERSION,
    Frame,
    FrameHeader,
)
from p2p.transport.connection import connect_and_send, start_server

def make_frame(rid: int, payload: bytes = b"data", type_: int = FRAME_TYPE_DATA) -> Frame:
    header = FrameHeader(
        version=PROTOCOL_VERSION, type=type_,
        flags=0, request_id=rid, payload_length=len(payload),
    )
    return Frame(header=header, payload=payload)

@pytest.mark.asyncio
async def test_ignores_wrong_request_id_then_accepts_correct() -> None:
    server = None
    
    async def malicious_handler(frame: Frame) -> Frame | None:
        return None

    async def custom_client_handler(reader, writer):
        await reader.read(24) 
        bad = make_frame(999, b"bad")
        writer.write(bad.encode())
        await writer.drain()
        good = make_frame(1, b"good", type_=FRAME_TYPE_HELLO_ACK)
        writer.write(good.encode())
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(custom_client_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    req = make_frame(1, b"req")
    resp = await connect_and_send(
        "127.0.0.1", port, req,
        max_frame_payload=1024,
        connection_timeout=5.0,
        expected_request_id=1,
    )

    server.close()
    await server.wait_closed()
    assert resp is not None
    assert resp.header.request_id == 1
    assert resp.payload == b"good"

@pytest.mark.asyncio
async def test_timeouts_if_no_correct_response() -> None:
    async def custom_client_handler(reader, writer):
        await reader.read(24)
        bad = make_frame(999, b"bad")
        writer.write(bad.encode())
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(custom_client_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    req = make_frame(1, b"req")
    resp = await connect_and_send(
        "127.0.0.1", port, req,
        max_frame_payload=1024,
        connection_timeout=0.5, 
        expected_request_id=1,
    )

    server.close()
    await server.wait_closed()

    assert resp is None 


@pytest.mark.asyncio
async def test_accepts_any_if_expected_is_none() -> None:
    async def custom_client_handler(reader, writer):
        await reader.read(24)
        bad = make_frame(999, b"bad")
        writer.write(bad.encode())
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(custom_client_handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    req = make_frame(1, b"req")
    resp = await connect_and_send(
        "127.0.0.1", port, req,
        max_frame_payload=1024,
        connection_timeout=5.0,
        expected_request_id=None,
    )

    server.close()
    await server.wait_closed()

    assert resp is not None
    assert resp.header.request_id == 999
