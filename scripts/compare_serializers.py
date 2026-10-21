import json
import msgpack
import statistics
import time

SAMPLES = {
    "PING": {"type": "PING", "request_id": 42, "from": "node1"},
    "FIND_NODE": {
        "type": "FIND_NODE", "request_id": 43,
        "target": "a" * 64,  
        "limit": 8,
    },
    "NodeRecord": {
        "node_id": "b" * 64,
        "public_key": "c" * 64,
        "host": "192.168.1.10",
        "port": 5001,
        "version": 1,
        "issued_at": "2026-10-21T10:00:00Z",
        "expires_at": "2026-10-21T11:00:00Z",
        "sequence_number": 5,
        "signature": "d" * 128,
    },
    "DATA": {
        "type": "DATA", "request_id": 44,
        "from": "node1", "to": "node2",
        "message_id": "e" * 32,
        "body": "Hello, this is a test message " * 10,
    },
}

def measure_json(data: dict, iterations: int = 10000) -> tuple[int, float, float]:
    encoded = json.dumps(data).encode("utf-8")
    size = len(encoded)
    
    t0 = time.perf_counter()
    for _ in range(iterations):
        json.dumps(data)
    t_encode = (time.perf_counter() - t0) / iterations * 1e6  
    
    t0 = time.perf_counter()
    for _ in range(iterations):
        json.loads(encoded)
    t_decode = (time.perf_counter() - t0) / iterations * 1e6
    
    return size, t_encode, t_decode

def measure_msgpack(data: dict, iterations: int = 10000) -> tuple[int, float, float]:
    encoded = msgpack.packb(data, use_bin_type=True)
    size = len(encoded)
    
    t0 = time.perf_counter()
    for _ in range(iterations):
        msgpack.packb(data, use_bin_type=True)
    t_encode = (time.perf_counter() - t0) / iterations * 1e6
    
    t0 = time.perf_counter()
    for _ in range(iterations):
        msgpack.unpackb(encoded, raw=False)
    t_decode = (time.perf_counter() - t0) / iterations * 1e6
    
    return size, t_encode, t_decode

def main() -> None:
    print(f"{'Sample':<12} {'JSON (bytes)':>14} {'msgpack (bytes)':>18} "
          f"{'Ratio':>8} {'JSON enc (µs)':>16} {'msgpack enc (µs)':>18}")
    print("-" * 95)
    
    total_json = 0
    total_msgpack = 0
    for name, data in SAMPLES.items():
        j_size, j_enc, j_dec = measure_json(data)
        m_size, m_enc, m_dec = measure_msgpack(data)
        total_json += j_size
        total_msgpack += m_size
        ratio = m_size / j_size if j_size else 0
        print(f"{name:<12} {j_size:>14} {m_size:>18} "
              f"{ratio:>8.2f} {j_enc:>16.2f} {m_enc:>18.2f}")
    
    print("-" * 95)
    print(f"{'TOTAL':<12} {total_json:>14} {total_msgpack:>18} "
          f"{total_msgpack/total_json:>8.2f}")


if __name__ == "__main__":
    main()
