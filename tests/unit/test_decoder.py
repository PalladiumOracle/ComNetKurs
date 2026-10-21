from __future__ import annotations
import random
import pytest
from p2p.protocol.decoder import StreamDecoder
from p2p.protocol.frame import (
    FRAME_TYPE_DATA,
    PROTOCOL_VERSION,
    Frame,
    FrameHeader,
)

def make_frame(request_id: int, payload: bytes) -> Frame:
    header = FrameHeader(
        version=PROTOCOL_VERSION,
        type=FRAME_TYPE_DATA,
        flags=0,
        request_id=request_id,
        payload_length=len(payload),
    )
    return Frame(header=header, payload=payload)

class TestStreamDecoder:
    def test_single_frame_in_one_chunk(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        frame = make_frame(1, b"hello")
        frames = decoder.feed(frame.encode())
        assert len(frames) == 1
        assert frames[0] == frame

    def test_two_frames_in_one_chunk(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        f1 = make_frame(1, b"first")
        f2 = make_frame(2, b"second")
        data = f1.encode() + f2.encode()
        frames = decoder.feed(data)
        assert len(frames) == 2
        assert frames[0] == f1
        assert frames[1] == f2

    def test_frame_split_into_pieces(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        frame = make_frame(1, b"hello there")
        data = frame.encode()
        collected: list[Frame] = []
        for i in range(len(data)):
            collected.extend(decoder.feed(data[i:i + 1]))
        assert len(collected) == 1
        assert collected[0] == frame

    def test_partial_header_then_rest(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        frame = make_frame(1, b"payload")
        data = frame.encode()
        assert decoder.feed(data[:10]) == []
        frames = decoder.feed(data[10:])
        assert len(frames) == 1
        assert frames[0] == frame

    def test_partial_payload_then_rest(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        frame = make_frame(1, b"payload")
        data = frame.encode()
        assert decoder.feed(data[:24 + 3]) == []
        frames = decoder.feed(data[24 + 3:])
        assert len(frames) == 1
        assert frames[0] == frame

    def test_100_frames_random_split(self) -> None:
        for iteration in range(20):
            rng = random.Random()   
            max_payload = 1024
            decoder = StreamDecoder(max_frame_payload=max_payload)
            frames_in = [
                make_frame(
                    request_id=i,
                    payload=f"payload-{i}".encode() * rng.randint(1, 10),
                )
                for i in range(100)
            ]
            for f in frames_in:
                assert f.header.payload_length <= max_payload
            stream = b"".join(f.encode() for f in frames_in)
            frames_out: list[Frame] = []
            pos = 0
            while pos < len(stream):
                chunk_size = rng.randint(1, 500)
                chunk = stream[pos:pos + chunk_size]
                pos += len(chunk)
                frames_out.extend(decoder.feed(chunk))
            assert len(frames_out) == 100, (
                f"Итерация {iteration}: ожидалось 100 кадров, "
                f"получено {len(frames_out)}"
            )
            for i, (fin, fout) in enumerate(zip(frames_in, frames_out)):
                assert fin == fout, (
                    f"Итерация {iteration}, кадр {i}: несовпадение"
                )

    def test_bad_version(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        header = FrameHeader(
            version=99, type=1, flags=0,
            request_id=1, payload_length=0,
        )
        with pytest.raises(ValueError, match="версия"):
            decoder.feed(header.encode())

    def test_payload_too_large(self) -> None:
        decoder = StreamDecoder(max_frame_payload=100)
        header = FrameHeader(
            version=PROTOCOL_VERSION, type=1, flags=0,
            request_id=1, payload_length=500,
        )
        with pytest.raises(ValueError, match="payload_length"):
            decoder.feed(header.encode())

    def test_buffered_bytes_diagnostic(self) -> None:
        decoder = StreamDecoder(max_frame_payload=1024)
        frame = make_frame(1, b"hello")
        data = frame.encode()
        decoder.feed(data[:5])
        assert decoder.buffered_bytes == 5
        decoder.feed(data[5:])
        assert decoder.buffered_bytes == 0
