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


def call(addr, msg, payload=b"", timeout=5.0):
    """Send one request to (host, port); return (response_dict, payload)."""
    with socket.create_connection(tuple(addr), timeout=timeout) as s:
        send(s, msg, payload)
        return recv(s)


class _Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            msg, payload = recv(self.request)
            try:
                resp, out = self.server.dispatch(msg, payload)
            except Exception as e:                       # report, never crash the server thread
                resp, out = {"ok": False, "error": f"{type(e).__name__}: {e}"}, b""
            send(self.request, resp, out)
        except (ConnectionError, OSError):
            pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, dispatch, host="127.0.0.1", port=0):
        super().__init__((host, port), _Handler)
        self.dispatch = dispatch
        self.addr = self.server_address
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self.shutdown()
        self.server_close()
