from __future__ import annotations
import logging
import time
from dataclasses import dataclass, field
from p2p.dht.xor import bucket_index, xor_distance

logger = logging.getLogger(__name__)

@dataclass
class Contact:
    node_id: bytes
    public_key: bytes
    host: str
    port: int
    last_seen: float = field(default_factory=time.time)

    @property
    def node_id_hex(self) -> str:
        return self.node_id.hex()

    def touch(self) -> None:
        self.last_seen = time.time()


class KBucket:
    def __init__(self, k: int = 8) -> None:
        self.k = k
        self.contacts: list[Contact] = []

    def add(self, contact: Contact) -> bool:
        for existing in self.contacts:
            if existing.node_id == contact.node_id:
                existing.touch()
                existing.host = contact.host
                existing.port = contact.port
                return True

        if len(self.contacts) < self.k:
            self.contacts.append(contact)
            return True
        return False

    def remove(self, node_id: bytes) -> bool:
        before = len(self.contacts)
        self.contacts = [c for c in self.contacts if c.node_id != node_id]
        return len(self.contacts) < before

    def get_lru(self) -> Contact | None:
        if not self.contacts:
            return None
        return min(self.contacts, key=lambda c: c.last_seen)

    def replace_lru(self, new_contact: Contact) -> Contact | None:
        lru = self.get_lru()
        if lru is None:
            return None
        self.contacts.remove(lru)
        self.contacts.append(new_contact)
        return lru


class RoutingTable:
    def __init__(self, local_id: bytes, k: int = 8) -> None:
        self.local_id = local_id
        self.k = k
        self.buckets: dict[int, KBucket] = {}

    def _bucket(self, idx: int) -> KBucket:
        if idx not in self.buckets:
            self.buckets[idx] = KBucket(k=self.k)
        return self.buckets[idx]

    def add_contact(self, contact: Contact) -> tuple[str, Contact | None]:
        if contact.node_id == self.local_id:
            return ("self", None)

        idx = bucket_index(self.local_id, contact.node_id)
        bucket = self._bucket(idx)
        for b in self.buckets.values():
            for c in b.contacts:
                if c.node_id == contact.node_id:
                    c.touch()
                    return ("updated", None)

        inserted = bucket.add(contact)
        if inserted:
            return ("added", None)
        return ("full", bucket.get_lru())

    def remove_contact(self, node_id: bytes) -> bool:
        idx = bucket_index(self.local_id, node_id)
        if idx in self.buckets:
            return self.buckets[idx].remove(node_id)
        return False

    def closest(self, target: bytes, limit: int) -> list[Contact]:
        all_contacts = [c for b in self.buckets.values() for c in b.contacts]
        all_contacts.sort(key=lambda c: xor_distance(target, c.node_id))
        return all_contacts[:limit]

    def all_contacts(self) -> list[Contact]:
        return [c for b in self.buckets.values() for c in b.contacts]

    def size(self) -> int:
        return sum(len(b.contacts) for b in self.buckets.values())
