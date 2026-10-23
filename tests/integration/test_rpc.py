from __future__ import annotations
import asyncio
from pathlib import Path
import pytest
from p2p.config import NodeConfig
from p2p.dht.routing import Contact
from p2p.node.node import Node

@pytest.fixture
async def two_nodes(tmp_path: Path):
    cfg1 = NodeConfig(
        node_id="node1", host="127.0.0.1", port=5101,
        state_dir=str(tmp_path / "state1"),
        protocol_version=1, max_frame_payload=1048576,
        connection_timeout=2.0, rpc_timeout=2.0,
    )
    cfg2 = NodeConfig(
        node_id="node2", host="127.0.0.1", port=5102,
        state_dir=str(tmp_path / "state2"),
        protocol_version=1, max_frame_payload=1048576,
        connection_timeout=2.0, rpc_timeout=2.0,
    )
    
    node1 = Node(cfg1)
    node2 = Node(cfg2)
    await node1.start()
    await node2.start()
    node1.add_contact(Contact(
        node_id=node2.identity.node_id,
        public_key=node2.identity.public_key_bytes,
        host=cfg2.host, port=cfg2.port
    ))
    node2.add_contact(Contact(
        node_id=node1.identity.node_id,
        public_key=node1.identity.public_key_bytes,
        host=cfg1.host, port=cfg1.port
    ))
    
    yield node1, node2, cfg1, cfg2
    await node1.stop()
    await node2.stop()

@pytest.mark.asyncio
async def test_ping_pong(two_nodes) -> None:
    node1, node2, cfg1, cfg2 = two_nodes
    assert await node1.ping(cfg2.host, cfg2.port) is True

@pytest.mark.asyncio
async def test_find_node(two_nodes) -> None:
    node1, node2, cfg1, cfg2 = two_nodes
    contacts = await node1.find_node(
        cfg2.host, cfg2.port, node2.identity.node_id, limit=8
    )
    
    assert len(contacts) >= 1
    assert contacts[0]["node_id"] == node1.identity.node_id_hex
