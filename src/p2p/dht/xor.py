from __future__ import annotations

def xor_distance(a: bytes, b: bytes) -> int:
    if len(a) != len(b):
        raise ValueError(f"Длины NodeID не совпадают: {len(a)} != {len(b)}")
    return int.from_bytes(
        bytes(x ^ y for x, y in zip(a, b)), byteorder="big"
    )

def bucket_index(local_id: bytes, remote_id: bytes) -> int:
    if local_id == remote_id:
        return -1
    distance = xor_distance(local_id, remote_id)
    bit_length = distance.bit_length()
    return 256 - bit_length


def sort_by_distance(target: bytes, ids: list[bytes]) -> list[bytes]:
    return sorted(ids, key=lambda nid: xor_distance(target, nid))
