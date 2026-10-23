from __future__ import annotations
import asyncio
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from p2p.config import NodeConfig
from p2p.dht.lookup import IterativeLookup
from p2p.dht.routing import Contact
from p2p.logging_setup import setup_logging
from p2p.node.node import Node

NODES = 5
BASE_PORT = 5200

async def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    setup_logging("bootstrap", "INFO", project_root / "logs")
    nodes = []
    for i in range(NODES):
        cfg = NodeConfig(
            node_id=f"node{i}",
            host="127.0.0.1",
            port=BASE_PORT + i,
            state_dir=str(project_root / "data" / "state" / f"e2_node{i}"),
            protocol_version=1,
            max_frame_payload=1048576,
            connection_timeout=3.0,
            rpc_timeout=3.0,
        )
        node = Node(cfg)
        nodes.append(node)
        
    for node in nodes:
        await node.start()
    print(f"\nЗапущено {NODES} узлов.")
    seed = nodes[0]
    for i in range(1, NODES):
        ok = await nodes[i].ping(seed.config.host, seed.config.port)
        print(f"node{i} -> node0 PING: {ok}")
        nodes[i].add_contact(Contact(
            node_id=seed.identity.node_id,
            public_key=seed.identity.public_key_bytes,
            host=seed.config.host, port=seed.config.port,
        ))
        lookup = IterativeLookup(nodes[i], nodes[i].identity.node_id)
        found = await lookup.run([Contact(
            node_id=seed.identity.node_id,
            public_key=seed.identity.public_key_bytes,
            host=seed.config.host, port=seed.config.port,
        )])
        print(
            f"node{i}: self-lookup нашёл {len(found)} контактов, "
            f"RPC={lookup.rpc_count}, итераций={lookup.iterations}"
        )
        for c in found:
            nodes[i].add_contact(c)
            
    target = nodes[4].identity.node_id
    print(f"\nnode1 ищет node4 (target={target.hex()[:16]}...)")
    lookup = IterativeLookup(nodes[1], target)
    found = await lookup.run([Contact(
        node_id=seed.identity.node_id,
        public_key=seed.identity.public_key_bytes,
        host=seed.config.host, port=seed.config.port,
    )])

    print(f"Найдено {len(found)} контактов. RPC={lookup.rpc_count}, "
          f"итераций={lookup.iterations}")
    for c in found[:3]:
        print(f"  {c.node_id_hex[:16]}... @ {c.host}:{c.port}")
    print("\nОстановка seed (node0)...")
    await seed.stop()
    print("seed остановлен. Повторный lookup от node1 к node4...")
    lookup2 = IterativeLookup(nodes[1], target)
    found2 = await lookup2.run([Contact(
        node_id=nodes[2].identity.node_id,
        public_key=nodes[2].identity.public_key_bytes,
        host=nodes[2].config.host, port=nodes[2].config.port,
    )])
    print(f"После остановки seed: найдено {len(found2)}, RPC={lookup2.rpc_count}")
    out_dir = project_root / "data" / "check" / "E2-8"
    out_dir.mkdir(parents=True, exist_ok=True)
    for i, node in enumerate(nodes):
        contacts = node.routing.all_contacts()
        data = [
            {
                "node_id": c.node_id_hex,
                "host": c.host,
                "port": c.port,
            }
            for c in contacts
        ]
        (out_dir / f"node{i}_routing.json").write_text(
            json.dumps(data, indent=2), encoding="utf-8",
        )
        print(f"node{i}: {len(contacts)} контактов в таблице")
    for node in nodes[1:]:
        await node.stop()
    print("\nГотово. Таблицы в data/check/E2-8/")


if __name__ == "__main__":
    asyncio.run(main())
