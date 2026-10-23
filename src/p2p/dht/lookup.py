from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING
from p2p.dht.routing import Contact
from p2p.dht.xor import xor_distance

if TYPE_CHECKING:
    from p2p.node.node import Node

logger = logging.getLogger(__name__)
ALPHA = 3      
K = 8          

@dataclass
class LookupCandidate:
    contact: Contact
    queried: bool = False
    responded: bool = False

class IterativeLookup:
    def __init__(self, node: Node, target_id: bytes) -> None:
        self.node = node
        self.target_id = target_id
        self.candidates: list[LookupCandidate] = []
        self.rpc_count = 0
        self.iterations = 0

    async def run(self, bootstrap_contacts: list[Contact]) -> list[Contact]:
        for c in bootstrap_contacts:
            self._add_candidate(c)

        while True:
            to_query = self._pick_next(ALPHA)
            if not to_query:
                break

            self.iterations += 1
            logger.info(
                "Lookup итерация %d: опрашивается %d узлов",
                self.iterations, len(to_query),
            )

            tasks = [
                self._query_one(cand) for cand in to_query
            ]
            await asyncio.gather(*tasks)

        responded = [c.contact for c in self.candidates if c.responded]
        responded.sort(
            key=lambda c: xor_distance(self.target_id, c.node_id)
        )
        return responded[:K]

    def _add_candidate(self, contact: Contact) -> None:
        for cand in self.candidates:
            if cand.contact.node_id == contact.node_id:
                return
        self.candidates.append(LookupCandidate(contact=contact))

    def _pick_next(self, alpha: int) -> list[LookupCandidate]:
        unqueried = [c for c in self.candidates if not c.queried]
        unqueried.sort(
            key=lambda c: xor_distance(self.target_id, c.contact.node_id)
        )
        picked = unqueried[:alpha]
        for c in picked:
            c.queried = True
        return picked

    async def _query_one(self, cand: LookupCandidate) -> None:
        self.rpc_count += 1
        try:
            contacts_dicts = await self.node.find_node(
                cand.contact.host, cand.contact.port,
                self.target_id, limit=K,
            )
            cand.responded = True
            for d in contacts_dicts:
                try:
                    c = Contact(
                        node_id=bytes.fromhex(d["node_id"]),
                        public_key=bytes.fromhex(d["public_key"]),
                        host=d["host"],
                        port=d["port"],
                    )
                    self._add_candidate(c)
                except Exception as exc:
                    logger.warning("Ошибка парсинга контакта: %s", exc)
        except Exception as exc:
            logger.warning(
                "FIND_NODE к %s:%d не удался: %s",
                cand.contact.host, cand.contact.port, exc,
            )
