from __future__ import annotations
import hashlib
from pathlib import Path
import pytest
from p2p.identity.identity import (
    compute_node_id,
    load_or_create_identity,
)

def test_first_run_creates_keys(tmp_path: Path) -> None:
    identity = load_or_create_identity(tmp_path)
    assert (tmp_path / "identity.key").exists()
    assert (tmp_path / "identity.pub").exists()
    assert len(identity.public_key_bytes) == 32
    assert len(identity.node_id) == 32

def test_restart_preserves_node_id(tmp_path: Path) -> None:
    id1 = load_or_create_identity(tmp_path)
    id2 = load_or_create_identity(tmp_path)
    assert id1.node_id == id2.node_id
    assert id1.public_key_bytes == id2.public_key_bytes

def test_node_id_is_sha256_of_public_key(tmp_path: Path) -> None:
    identity = load_or_create_identity(tmp_path)
    expected = hashlib.sha256(identity.public_key_bytes).digest()
    assert identity.node_id == expected

def test_sign_and_verify(tmp_path: Path) -> None:
    identity = load_or_create_identity(tmp_path)
    data = b"test message"
    signature = identity.sign(data)
    assert len(signature) == 64
    assert identity.verify(identity.public_key_bytes, signature, data) is True
    assert identity.verify(identity.public_key_bytes, signature, b"other") is False

def test_compute_node_id() -> None:
    pub = b"\x00" * 32
    expected = hashlib.sha256(pub).digest()
    assert compute_node_id(pub) == expected
