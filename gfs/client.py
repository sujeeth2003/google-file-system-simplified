"""GFS client library: talks to the master for metadata, then straight to chunkservers for data."""
import uuid

from . import protocol


class GFSError(Exception):
    pass


class GFSClient:
    def __init__(self, master_addr):
        self.master = tuple(master_addr)
        cfg = self._master({"op": "config"})
        self.chunk_size, self.block_size = cfg["chunk_size"], cfg["block_size"]
        self._cache = {}            # (path, index) -> chunk_info  (client-side location cache)

    # ------------------------------------------------------------------ master calls
    def _master(self, msg):
        resp, _ = protocol.call(self.master, msg)
        if not resp.get("ok"):
            raise GFSError(resp.get("error", "master error"))
        return resp

    def create(self, path): self._master({"op": "create", "path": path})
    def delete(self, path): self._master({"op": "delete", "path": path}); self._cache.clear()
    def list(self, prefix=""): return self._master({"op": "list", "prefix": prefix})["files"]
    def stat(self, path): return self._master({"op": "stat", "path": path})["length"]

    def _chunk(self, path, index, create=False, mutate=False, refresh=False):
        key = (path, index)
        if not refresh and not mutate and key in self._cache:
            return self._cache[key]
        info = self._master({"op": "chunk_info", "path": path, "index": index, "create": create, "mutate": mutate})
        self._cache[key] = info
        return info

    # ------------------------------------------------------------------ reads
    def read(self, path, offset, length):
        out = bytearray()
        while length > 0:
            idx, off = divmod(offset, self.chunk_size)
            n = min(length, self.chunk_size - off)
            out += self._read_chunk(path, idx, off, n)
            offset += n; length -= n
        return bytes(out)

    def read_all(self, path):
        return self.read(path, 0, self.stat(path))

    def _read_chunk(self, path, idx, off, n):
        last = None
        for attempt in range(2):                           # second attempt re-asks the master (stale cache / dead replica)
            info = self._chunk(path, idx, refresh=attempt > 0)
            for addr in info["replicas"]:
                try:
                    r, data = protocol.call(addr, {"op": "read", "handle": info["handle"], "offset": off, "length": n})
                except OSError as e:
                    last = e; continue
                if r.get("ok"):
                    return data + b"\0" * (n - len(data))       # region past what the replica has yet reads as zeros
                last = r.get("error")                          # e.g. checksum mismatch: fall through to the next replica
        raise GFSError(f"read failed on all replicas: {last}")

    # ------------------------------------------------------------------ writes
    def write(self, path, offset, data):
        pos = 0
        while pos < len(data):
            idx, off = divmod(offset + pos, self.chunk_size)
            n = min(len(data) - pos, self.chunk_size - off)
            self._mutate(path, idx, "write", data[pos:pos + n], off)
            pos += n

    def append(self, path, data):
        """Record append: atomically append `data` as one record at an offset GFS chooses; returns that file offset.
        Records larger than a quarter chunk are rejected (paper: keeps padding waste bounded)."""
        if len(data) > self.chunk_size // 4:
            raise GFSError("record too large for record append")
        idx = self._master({"op": "last_chunk", "path": path})["index"]
        for _ in range(8):
            status = self._mutate(path, idx, "append", data, None)
            if status["status"] == "done":
                return idx * self.chunk_size + status["offset"]
            idx += 1                                           # chunk was padded: retry on the next chunk
        raise GFSError("append kept retrying")

