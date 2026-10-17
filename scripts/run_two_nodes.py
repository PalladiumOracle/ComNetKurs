from __future__ import annotations
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from p2p.config import load_config
from p2p.logging_setup import setup_logging
from p2p.node.node import Node

async def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    cfg1 = load_config(project_root / "configs" / "node1.yaml")

    setup_logging(
        node_id=cfg1.node_id,
        log_level=cfg1.log_level,
        log_dir=project_root / "logs",
    )

    print("\nСценарий 1: штатный обмен кадрами")
    node1 = Node(cfg1)
    await node1.start()
    response = await node1.send_to(
        host=cfg1.host, port=cfg1.port,
        message="Hello from node2",
    )
    if response is not None:
        text = response.payload.decode("utf-8")
        print(f"node2 получил ответ: type={response.header.type}, "
              f"request_id={response.header.request_id}, text={text!r}")
    else:
        print("node2 не получил ответ")

    print("\nСценарий 2: тайм-аут чтения")
    try:
        reader, writer = await asyncio.open_connection(cfg1.host, cfg1.port)
        print("Подключение к node1 с тайм-аутом")
        await asyncio.sleep(cfg1.connection_timeout + 1.0)
        writer.close()
        await writer.wait_closed()
        print("Соединение закрыто после ожидания")
    except Exception as exc:
        print(f"Ошибка: {exc}")

    await node1.stop()
    print("\nГотово. Журналы: logs/node1.log, logs/node1.jsonl")


if __name__ == "__main__":
    asyncio.run(main())
