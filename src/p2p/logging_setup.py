from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path


class JsonFormatter(logging.Formatter):
    def __init__(self, node_id: str) -> None:
        super().__init__()
        self.node_id = node_id

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "node": self.node_id,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "data", None)
        if extra is not None:
            payload["data"] = extra
        return json.dumps(payload, ensure_ascii=False)


def setup_logging(
    node_id: str,
    log_level: str = "INFO",
    log_dir: str | Path = "logs",
) -> logging.Logger:
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(log_level.upper())

    for handler in list(root.handlers):
        root.removeHandler(handler)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(console)

    text_file = logging.FileHandler(
        log_dir / f"{node_id}.log", encoding="utf-8"
    )
    text_file.setFormatter(logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    root.addHandler(text_file)

    json_file = logging.FileHandler(
        log_dir / f"{node_id}.jsonl", encoding="utf-8"
    )
    json_file.setFormatter(JsonFormatter(node_id=node_id))
    root.addHandler(json_file)

    return root
