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

