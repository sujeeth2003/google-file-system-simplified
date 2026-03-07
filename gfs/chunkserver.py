"""GFS chunkserver: stores chunks as ordinary files, serves reads, applies mutations in a primary-assigned order.

Write path (paper figure 2, simplified):
  1. client pushes the DATA to every replica (buffered here under a data id, nothing applied yet)
  2. client sends the mutation request to the PRIMARY
  3. the primary picks an offset/serial order, applies the buffered data, and forwards the same
     instruction to the secondaries, which apply it in that same order
  4. primary replies once every replica has applied it
Data flow (step 1) is decoupled from control flow (steps 2-4).

Integrity: every chunk keeps a CRC32 per block; reads verify the blocks they touch and a mismatch is reported to the master,
which then re-replicates the chunk from a healthy replica.
"""
import json
import os
import threading
import time
import zlib

from . import protocol


class ChecksumError(Exception):
    """A block's CRC32 does not match: the replica is corrupt (distinct from ordinary I/O errors)."""


class ChunkServer:
    def __init__(self, root, master_addr, heartbeat_interval=0.3, scrub_interval=2.0, host="127.0.0.1", port=0):
        os.makedirs(root, exist_ok=True)
        self.root, self.master = root, tuple(master_addr)
        cfg, _ = protocol.call(self.master, {"op": "config"})
        self.chunk_size, self.block_size = cfg["chunk_size"], cfg["block_size"]
        self.buffers = {}                                   # data_id -> bytes (pushed, not yet applied)
        self.locks = {}                                     # handle -> Lock (serialises mutations per chunk)
        self.meta_lock = threading.Lock()
        self.server = protocol.Server(self._dispatch, host, port)
        self.addr = self.server.addr
        self.alive = True
        self._stop = threading.Event()
        self._hb = threading.Thread(target=self._heartbeat_loop, args=(heartbeat_interval,), daemon=True)
        self._scrub = threading.Thread(target=self._scrub_loop, args=(scrub_interval,), daemon=True)

    def start(self):
        self.server.start()
        self._hb.start()
        self._scrub.start()
        return self

    def stop(self):                                          # simulate a crash: stop serving and stop heartbeating
        self.alive = False
        self._stop.set()
        self.server.stop()

    # ------------------------------------------------------------------ on-disk layout
    def _path(self, h): return os.path.join(self.root, h + ".chunk")
    def _meta_path(self, h): return os.path.join(self.root, h + ".meta")
    def _lock(self, h):
        with self.meta_lock:
            return self.locks.setdefault(h, threading.Lock())

    def _load_meta(self, h):
        with open(self._meta_path(h)) as f:
            return json.load(f)

    def _save_meta(self, h, meta):
        tmp = self._meta_path(h) + ".tmp"
        with open(tmp, "w") as f: json.dump(meta, f)
        os.replace(tmp, self._meta_path(h))

    def _crc_blocks(self, data):
        return [zlib.crc32(data[i:i + self.block_size]) for i in range(0, len(data), self.block_size)]

    def _has(self, h): return os.path.exists(self._path(h)) and os.path.exists(self._meta_path(h))

    # ------------------------------------------------------------------ heartbeat
    def _inventory(self):
        inv = {}
        for fn in os.listdir(self.root):
            if fn.endswith(".meta"):
                h = fn[:-5]
                try:
                    inv[h] = [self._load_meta(h)["version"], os.path.getsize(self._path(h))]
                except (OSError, ValueError, KeyError):
                    pass
        return inv

    def _heartbeat_loop(self, interval):
        while not self._stop.is_set():
            try:
                resp, _ = protocol.call(self.master, {"op": "heartbeat", "addr": list(self.addr), "chunks": self._inventory()}, timeout=2)
                for h in resp.get("delete", []): self._drop(h)
            except Exception:              # never let the heartbeat thread die (master restarting, torn reply, ...)
                pass
            self._stop.wait(interval)

    def _report_corrupt(self, h):
        try:                                                    # the master drops this replica and re-replicates from a good one
            protocol.call(self.master, {"op": "report_corrupt", "handle": h, "addr": list(self.addr)}, timeout=2)
        except OSError:
            pass

    def _scrub_loop(self, interval):
        """Background scrubbing: verify every block checksum of every chunk, so a rotted replica is found
        even if nobody reads it (paper section 5.2)."""
        while not self._stop.wait(interval):
            for h in list(self._inventory()):
                if self._stop.is_set(): return
                try:
                    with self._lock(h):
                        self._read_verified(h, 0, os.path.getsize(self._path(h)))
                except ChecksumError:
                    self._report_corrupt(h)
                except Exception:
                    pass

    def _drop(self, h):
        for p in (self._path(h), self._meta_path(h)):
            try: os.remove(p)
            except OSError: pass

    # ------------------------------------------------------------------ RPC dispatch
    def _dispatch(self, msg, payload):
        if not self.alive:
            raise ConnectionError("server stopped")
        fn = getattr(self, "rpc_" + msg["op"], None)
        if fn is None:
            return {"ok": False, "error": f"unknown op {msg['op']}"}, b""
        out = fn(msg, payload)
        return out if isinstance(out, tuple) else (out, b"")

    def rpc_create_chunk(self, m, _):
        h = m["handle"]
        with self._lock(h):
            open(self._path(h), "wb").close()
            self._save_meta(h, {"version": m["version"], "crc": []})
        return {"ok": True}

    def rpc_set_version(self, m, _):
        h = m["handle"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        with self._lock(h):
            meta = self._load_meta(h); meta["version"] = m["version"]; self._save_meta(h, meta)
        return {"ok": True}

    def rpc_delete_chunk(self, m, _):
        self._drop(m["handle"]); return {"ok": True}

    def rpc_push(self, m, data):
        self.buffers[m["data_id"]] = data
        return {"ok": True}

    # ---- reads ----------------------------------------------------------------
    def _read_verified(self, h, offset, length):
        with open(self._path(h), "rb") as f:
            raw = f.read()
        meta = self._load_meta(h)
        first, last = offset // self.block_size, (offset + max(length, 1) - 1) // self.block_size
        for b in range(first, min(last, len(meta["crc"]) - 1) + 1):
            if zlib.crc32(raw[b * self.block_size:(b + 1) * self.block_size]) != meta["crc"][b]:
                raise ChecksumError(f"checksum mismatch in block {b}")
        return raw[offset:offset + length]

    def rpc_read(self, m, _):
        h = m["handle"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        try:
            with self._lock(h):                                 # do not verify while a mutation is half-applied
                data = self._read_verified(h, m["offset"], m["length"])
        except ChecksumError as e:
            self._report_corrupt(h)
            return {"ok": False, "error": str(e)}
        return {"ok": True, "version": self._load_meta(h)["version"]}, data

    def rpc_read_all(self, m, _):
        h = m["handle"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        with self._lock(h):
            n = os.path.getsize(self._path(h))
            return {"ok": True, "version": self._load_meta(h)["version"]}, self._read_verified(h, 0, n)

    # ---- mutations --------------------------------------------------------------
    def _apply(self, h, offset, data):
        """Write data at offset (zero-filling any gap), then recompute the CRCs of every touched block."""
        with open(self._path(h), "r+b") as f:
            f.seek(0, os.SEEK_END)
            if f.tell() < offset: f.write(b"\0" * (offset - f.tell()))
            f.seek(offset); f.write(data)
            f.seek(0); raw = f.read()
        meta = self._load_meta(h)
        crcs = self._crc_blocks(raw)
        meta["crc"] = crcs
        self._save_meta(h, meta)

    def rpc_apply(self, m, _):                                   # executed on secondaries, in primary-chosen order
        h = m["handle"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        with self._lock(h):
            if self._load_meta(h)["version"] != m["version"]: return {"ok": False, "error": "stale replica"}
            if m.get("is_pad"):
                data = b"\0" * m["pad"]
            else:
                data = self.buffers.pop(m["data_id"], None)
                if data is None: return {"ok": False, "error": "data not pushed"}
            self._apply(h, m["offset"], data)
        return {"ok": True}

    def rpc_length(self, m, _):
        h = m["handle"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        with self._lock(h):                                      # wait for an in-progress mutation / clone to finish
            return {"ok": True, "length": os.path.getsize(self._path(h))}

    def rpc_mutate(self, m, _):                                  # executed on the primary
        h, kind = m["handle"], m["kind"]
        if not self._has(h): return {"ok": False, "error": "no such chunk"}
        data = self.buffers.get(m["data_id"])
        if data is None: return {"ok": False, "error": "data not pushed"}
        with self._lock(h):                                      # serial order = lock acquisition order
            if self._load_meta(h)["version"] != m["version"]: return {"ok": False, "error": "stale primary"}
            length = os.path.getsize(self._path(h))
            if kind == "append":
                if length + len(data) > self.chunk_size:         # record would straddle the chunk: pad and retry on the next one
                    pad = self.chunk_size - length
                    return self._commit(m, h, length, None, pad, "retry_next_chunk")
                offset = length
            else:
                offset = m["offset"]
                if offset + len(data) > self.chunk_size: return {"ok": False, "error": "write crosses chunk boundary"}
            return self._commit(m, h, offset, data, 0, None)

