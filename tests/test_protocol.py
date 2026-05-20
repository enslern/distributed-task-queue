import pytest
from shared import protocol


def test_encode_decode_roundtrip():
    msg = {"type": "SUBMIT_TASK", "task_id": "abc123"}
    encoded = protocol.encode(msg)
    # strip 4-byte header
    length = int.from_bytes(encoded[:4], byteorder="big")
    decoded = protocol.decode(encoded[4:])
    assert decoded == msg

def test_encode_produces_bytes():
    msg = {"type": "ACK"}
    assert isinstance(protocol.encode(msg), bytes)

def test_length_header_correct():
    msg = {"type": "ACK"}
    encoded = protocol.encode(msg)
    header = encoded[:4]
    length = int.from_bytes(header, byteorder="big")
    assert length == len(encoded) - 4

def test_decode_various_types():
    msg = {"type": "TASK_RESULT", "result": 42, "status": "success"}
    encoded = protocol.encode(msg)
    decoded = protocol.decode(encoded[4:])
    assert decoded["result"] == 42
    assert decoded["status"] == "success"