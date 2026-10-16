from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import yaml


@dataclass(frozen=True)
class NodeConfig:
    node_id: str                      #логическое имя узла
    host: str                         #адрес прослушивания
    port: int                         #TCP-порт
    protocol_version: int             #версия протокола
    max_frame_payload: int            #максимальный размер payload, байт
    connection_timeout: float         #тайм-аут соединения, секунды
    rpc_timeout: float                #тайм-аут ожидания ответа на RPC, секунды
    log_level: str = "INFO"           #уровень логирования


def load_config(path: str | Path) -> NodeConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Конфиг не найден: {path}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Конфиг должен быть словарём: {path}")

    required = [
        "node_id", "host", "port", "protocol_version",
        "max_frame_payload", "connection_timeout", "rpc_timeout",
    ]
    missing = [key for key in required if key not in raw]
    if missing:
        raise ValueError(f"В конфиге {path} отсутствуют поля: {missing}")

    return NodeConfig(
        node_id=raw["node_id"],
        host=raw["host"],
        port=int(raw["port"]),
        protocol_version=int(raw["protocol_version"]),
        max_frame_payload=int(raw["max_frame_payload"]),
        connection_timeout=float(raw["connection_timeout"]),
        rpc_timeout=float(raw["rpc_timeout"]),
        log_level=raw.get("log_level", "INFO"),
    )
