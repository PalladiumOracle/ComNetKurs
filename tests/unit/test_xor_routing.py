from __future__ import annotations
import time
import pytest
from p2p.dht.routing import Contact, KBucket, RoutingTable
from p2p.dht.xor import bucket_index, sort_by_distance, xor_distance

def test_xor_distance_symmetric() -> None:
    a = b"\x00" * 32
    b = b"\x01" + b"\x00" * 31
    assert xor_distance(a, b) == xor_distance(b, a)
    assert xor_distance(a, a) == 0

def test_xor_distance_length_mismatch() -> None:
    with pytest.raises(ValueError):
        xor_distance(b"\x00" * 32, b"\x00" * 31)

def test_bucket_index() -> None:
    local = b"\x00" * 32
    assert bucket_index(local, b"\x80" + b"\x00" * 31) == 0
    assert bucket_index(local, b"\x40" + b"\x00" * 31) == 1
    assert bucket_index(local, b"\x00" * 31 + b"\x01") == 255
    assert bucket_index(local, local) == -1

def test_sort_by_distance() -> None:
    target = b"\x00" * 32
    ids = [
        b"\xff" + b"\x00" * 31, 
        b"\x00" * 32,            
        b"\x00" * 31 + b"\x01",  
    ]
    sorted_ids = sort_by_distance(target, ids)
    assert sorted_ids[0] == b"\x00" * 32
    assert sorted_ids[1] == b"\x00" * 31 + b"\x01"
    assert sorted_ids[2] == b"\xff" + b"\x00" * 31

def make_contact(node_id: bytes, host: str = "127.0.0.1", port: int = 5001) -> Contact:
    return Contact(
        node_id=node_id, public_key=b"\x00" * 32,
        host=host, port=port,
    )

class TestKBucket:
    def test_capacity(self) -> None:
        bucket = KBucket(k=3)
        for i in range(3):
            assert bucket.add(make_contact(bytes([i]) + b"\x00" * 31)) is True
        assert bucket.add(make_contact(b"\xff" * 32)) is False
        assert len(bucket.contacts) == 3

    def test_update_existing(self) -> None:
        bucket = KBucket(k=3)
        c = make_contact(b"\x01" * 32)
        bucket.add(c)
        c.last_seen = 0  
        c2 = make_contact(b"\x01" * 32, host="10.0.0.1")
        assert bucket.add(c2) is True
        assert len(bucket.contacts) == 1
        assert bucket.contacts[0].host == "10.0.0.1"
        assert bucket.contacts[0].last_seen > 0  

    def test_lru_and_replace(self) -> None:
        bucket = KBucket(k=2)
        c1 = make_contact(b"\x01" * 32)
        c1.last_seen = 100
        c2 = make_contact(b"\x02" * 32)
        c2.last_seen = 200
        bucket.add(c1)
        bucket.add(c2)

        lru = bucket.get_lru()
        assert lru is not None and lru.node_id == c1.node_id

        new_c = make_contact(b"\x03" * 32)
        replaced = bucket.replace_lru(new_c)
        assert replaced is not None and replaced.node_id == c1.node_id
        assert len(bucket.contacts) == 2
        assert any(c.node_id == new_c.node_id for c in bucket.contacts)

class TestRoutingTable:
    def test_add_self_rejected(self) -> None:
        local = b"\x00" * 32
        table = RoutingTable(local, k=8)
        status, _ = table.add_contact(make_contact(local))
        assert status == "self"

    def test_add_duplicate(self) -> None:
        local = b"\x00" * 32
        table = RoutingTable(local, k=8)
        c = make_contact(b"\x01" * 32)
        s1, _ = table.add_contact(c)
        s2, _ = table.add_contact(c)
        assert s1 == "added"
        assert s2 == "updated"
        assert table.size() == 1

    def test_full_bucket_returns_lru(self) -> None:
        local = b"\x00" * 32
        table = RoutingTable(local, k=2)        
        c1 = make_contact(b"\x80" + b"\x00" * 31)
        c2 = make_contact(b"\x81" + b"\x00" * 31)
        c3 = make_contact(b"\x82" + b"\x00" * 31)
        c1.last_seen = 100
        c2.last_seen = 200
        table.add_contact(c1)
        table.add_contact(c2)

        status, lru = table.add_contact(c3)
        assert status == "full"
        assert lru is not None and lru.node_id == c1.node_id

    def test_closest(self) -> None:
        local = b"\x00" * 32
        table = RoutingTable(local, k=8)
        for i in range(1, 6):
            table.add_contact(make_contact(bytes([i]) + b"\x00" * 31))
        closest = table.closest(b"\x00" * 32, limit=3)
        assert len(closest) == 3
        assert closest[0].node_id == b"\x01" + b"\x00" * 31
