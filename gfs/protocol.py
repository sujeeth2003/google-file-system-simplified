"""Tiny RPC layer: one request/response per TCP connection.

Wire format: 4-byte big-endian length of a JSON header, the JSON header (which carries the payload
length under "_n"), then the raw payload bytes. Keeping bulk data out of JSON is the point: the same
framing carries 64 MB chunks and small control messages.
"""
import json
import socket
import socketserver
import struct
import threading


def _recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        part = sock.recv(n - len(buf))
        if not part:
            raise ConnectionError("connection closed mid-message")
        buf += part
    return bytes(buf)


def send(sock, msg, payload=b""):
    m = dict(msg)
    m["_n"] = len(payload)
    body = json.dumps(m).encode()
    sock.sendall(struct.pack(">I", len(body)) + body + payload)


def recv(sock):
    (n,) = struct.unpack(">I", _recv_exact(sock, 4))
    msg = json.loads(_recv_exact(sock, n))
    payload = _recv_exact(sock, msg.pop("_n")) if msg.get("_n") else b""
    msg.pop("_n", None)
    return msg, payload


