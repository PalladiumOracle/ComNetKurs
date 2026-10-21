from __future__ import annotations
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from p2p.config import load_config
from p2p.logging_setup import setup_logging
from p2p.node.node import Node
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    PROTOCOL_VERSION,
    Frame,
    FrameHeader,
)
from p2p.transport.connection import connect_and_send

def make_frame(rid: int, payload: bytes, type_: int = FRAME_TYPE_DATA) -> Frame:
    return Frame(
        header=FrameHeader(
            version=PROTOCOL_VERSION, type=type_,
            flags=0, request_id=rid, payload_length=len(payload),
        ),
        payload=payload,
    )

async def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cfg = load_config(project_root / "configs" / "node1.yaml")
    setup_logging(cfg.node_id, cfg.log_level, project_root / "logs")

    node = Node(cfg)
    await node.start()

    async def client_unknown_type():
        print("\n[Клиент 1] отправляет кадр с неизвестным type=200")
        resp = await connect_and_send(
            cfg.host, cfg.port,
            make_frame(1, b"garbage", type_=200),
            max_frame_payload=cfg.max_frame_payload,
            connection_timeout=1.0,
            expected_request_id=None,
        )
        print(f"[Клиент 1] ответ: {resp}")

    async def client_partial():
        print("\n[Клиент 2] отправляет 10 байт заголовка и обрывает соединение")
        reader, writer = await asyncio.open_connection(cfg.host, cfg.port)
        writer.write(b"\x01\x10\x00\x00" + b"\x00" * 6)
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        print("[Клиент 2] оборвал соединение.")

    async def client_normal():
        await asyncio.sleep(0.2)
        print("\n[Клиент 3] отправляет нормальный кадр")
        resp = await connect_and_send(
            cfg.host, cfg.port,
            make_frame(3, b"normal", type_=FRAME_TYPE_DATA),
            max_frame_payload=cfg.max_frame_payload,
            connection_timeout=2.0,
            expected_request_id=3,
        )
        print(f"[Клиент 3] ответ: type={resp.header.type if resp else None}, "
              f"payload={resp.payload if resp else None}")

    await asyncio.gather(
        client_unknown_type(),
        client_partial(),
        client_normal(),
    )
    await node.stop()
    print("\nЖурналы: logs/node1.*")


if __name__ == "__main__":
    asyncio.run(main())
