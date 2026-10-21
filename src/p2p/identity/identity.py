from __future__ import annotations
import hashlib
import logging
from dataclasses import dataclass
from pathlib import Path
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Identity:    
    private_key: ed25519.Ed25519PrivateKey
    public_key_bytes: bytes
    node_id: bytes   
    node_id_hex: str 

    def sign(self, data: bytes) -> bytes:        
        return self.private_key.sign(data)

    @staticmethod
    def verify(public_key_bytes: bytes, signature: bytes, data: bytes) -> bool:
        try:
            pub = ed25519.Ed25519PublicKey.from_public_bytes(public_key_bytes)
            pub.verify(signature, data)
            return True
        except Exception:
            return False

def compute_node_id(public_key_bytes: bytes) -> bytes:
    return hashlib.sha256(public_key_bytes).digest()

def load_or_create_identity(state_dir: str | Path) -> Identity:
    state_dir = Path(state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)

    key_path = state_dir / "identity.key"
    pub_path = state_dir / "identity.pub"

    if key_path.exists() and pub_path.exists():
        logger.info("Загрузка существующей идентичности из %s", state_dir)
        private_key = serialization.load_pem_private_key(
            key_path.read_bytes(), password=None
        )
        public_key_bytes = pub_path.read_bytes()
    else:
        logger.info("Создание новой идентичности в %s", state_dir)
        private_key = ed25519.Ed25519PrivateKey.generate()
        public_key = private_key.public_key()
        public_key_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )
        key_path.write_bytes(private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))
        pub_path.write_bytes(public_key_bytes)
        try:
            key_path.chmod(0o600)
        except Exception:
            pass

    node_id = compute_node_id(public_key_bytes)
    node_id_hex = node_id.hex()
    logger.info("NodeID: %s", node_id_hex)
    return Identity(
        private_key=private_key,
        public_key_bytes=public_key_bytes,
        node_id=node_id,
        node_id_hex=node_id_hex,
    )
