from __future__ import annotations
import asyncio
import pytest
from p2p.config import NodeConfig
from p2p.node.node import Node
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    PROTOCOL_VERSION,
    Frame,
    FrameHeader,
)
from p2p.transport.connection import connect_and_send

def make_frame(rid: int, payload: bytes, type_: int = FRAME_TYPE_DATA) -> Frame:
    header = FrameHeader(
        version=PROTOCOL_VERSION, type=type_,
        flags=0, request_id=rid, payload_length=len(payload),
    )
    return Frame(header=header, payload=payload)


@pytest.fixture
async def running_node():
    cfg = NodeConfig(
        node_id="test_node", host="127.0.0.1", port=5099,
        protocol_version=1, max_frame_payload=1024,
        connection_timeout=2.0, rpc_timeout=2.0,
    )
    node = Node(cfg)
    await node.start()
    yield node, cfg
    await node.stop()

@pytest.mark.asyncio
async def test_unknown_type_not_fatal(running_node) -> None:
    node, cfg = running_node
    bad_frame = make_frame(1, b"garbage", type_=200)
    resp = await connect_and_send(
        cfg.host, cfg.port, bad_frame,
        max_frame_payload=cfg.max_frame_payload,
        connection_timeout=1.0,
        expected_request_id=None,
    )
    assert resp is None
    assert node._server is not None
    assert node._server.is_serving()

@pytest.mark.asyncio
async def test_bad_payload_not_fatal(running_node) -> None:
    """Payload не UTF-8 не роняет узел."""
    node, cfg = running_node
    bad_payload = b"\xff\xfe\x00\x01"  
    frame = make_frame(1, bad_payload, type_=FRAME_TYPE_DATA)
    resp = await connect_and_send(
        cfg.host, cfg.port, frame,
        max_frame_payload=cfg.max_frame_payload,
        connection_timeout=2.0,
        expected_request_id=1,
    )
    assert resp is not None
    assert resp.header.type == 2  # HELLO_ACK
    assert b"ACK" in resp.payload

@pytest.mark.asyncio
async def test_partial_frame_disconnect(running_node) -> None:
    node, cfg = running_node
    reader, writer = await asyncio.open_connection(cfg.host, cfg.port)
    writer.write(b"\x01\x10\x00\x00" + b"\x00" * 6)  
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    await asyncio.sleep(0.2)
    assert node._server.is_serving()
    good = make_frame(2, b"normal", type_=FRAME_TYPE_DATA)
    resp = await connect_and_send(
        cfg.host, cfg.port, good,
        max_frame_payload=cfg.max_frame_payload,
        connection_timeout=2.0,
        expected_request_id=2,
    )
    assert resp is not None

@pytest.mark.asyncio
async def test_concurrent_flood_does_not_block(running_node) -> None:
    node, cfg = running_node

    async def flooder():
        reader, writer = await asyncio.open_connection(cfg.host, cfg.port)
        for i in range(100):
            writer.write(make_frame(i, b"x" * 10).encode())
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.close()
        await writer.wait_closed()

    flood_task = asyncio.create_task(flooder())
    await asyncio.sleep(0.05)  
    good = make_frame(999, b"normal", type_=FRAME_TYPE_DATA)
    resp = await connect_and_send(
        cfg.host, cfg.port, good,
        max_frame_payload=cfg.max_frame_payload,
        connection_timeout=3.0,
        expected_request_id=999,
    )
    await flood_task
    assert resp is not None
