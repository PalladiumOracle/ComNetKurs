from __future__ import annotations
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from p2p.logging_setup import setup_logging
from p2p.protocol.frame import FRAME_TYPE_HELLO_ACK, PROTOCOL_VERSION, Frame, FrameHeader
from p2p.transport.connection import connect_and_send

def make_frame(rid: int, payload: bytes, type_: int = 16) -> Frame:
    return Frame(
        header=FrameHeader(version=PROTOCOL_VERSION, type=type_, flags=0,
                           request_id=rid, payload_length=len(payload)),
        payload=payload,
    )

async def main() -> None:
    setup_logging("e1_6_demo", "INFO", "logs")

    async def malicious_server(reader, writer):
        await reader.read(24)
        writer.write(make_frame(999, b"garbage").encode())
        await writer.drain()
        await asyncio.sleep(0.1)
        writer.write(make_frame(1, b"real_ack", type_=FRAME_TYPE_HELLO_ACK).encode())
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(malicious_server, "127.0.0.1", 5055)
    print("Запущен отправитель чужого пакета на 127.0.0.1:5055")
    print("Клиент отправляет запрос с request_id=1 и ждёт ответ")
    resp = await connect_and_send(
        "127.0.0.1", 5055, make_frame(1, b"req"),
        max_frame_payload=1024, connection_timeout=5.0,
        expected_request_id=1,
    )

    if resp:
        print(f"\nКлиент принял ответ с request_id={resp.header.request_id}, payload={resp.payload!r}")
    else:
        print("\nОшибка: клиент не получил ответ")

    server.close()
    await server.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
