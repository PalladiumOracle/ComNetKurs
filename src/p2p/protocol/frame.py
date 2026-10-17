from __future__ import annotations
from dataclasses import dataclass

HEADER_SIZE: int = 24
PROTOCOL_VERSION: int = 1
FRAME_TYPE_HELLO: int = 1       
FRAME_TYPE_HELLO_ACK: int = 2   
FRAME_TYPE_BYE: int = 3         
FRAME_TYPE_BYE_ACK: int = 4     
FRAME_TYPE_DATA: int = 16       


@dataclass(frozen=True)
class FrameHeader:
    version: int
    type: int
    flags: int
    request_id: int
    payload_length: int
    reserved: int = 0
