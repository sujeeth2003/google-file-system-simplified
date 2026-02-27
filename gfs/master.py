"""GFS master: namespace + chunk metadata + chunk placement, leases, re-replication.

What the master stores (as in the paper):
  * file namespace and file -> ordered list of chunk handles   (persisted in an operation log)
  * chunk version numbers                                       (persisted; used to detect stale replicas)
  * chunk LOCATIONS                                             (NOT persisted: rebuilt from chunkserver heartbeats)
Data never flows through the master, which is what keeps it from becoming the bottleneck: clients ask it
"which chunkservers hold chunk N of file F?", cache the answer, and then talk to chunkservers directly.
"""
import json
import os
import random
import threading
import time
import uuid

from . import protocol


class Master:
    def __init__(self, root, chunk_size=64 * 1024 * 1024, block_size=64 * 1024, replicas=3,
                 heartbeat_timeout=2.0, lease_seconds=10.0, maintenance_interval=0.5, host="127.0.0.1", port=0):
        os.makedirs(root, exist_ok=True)
        self.cfg = {"chunk_size": chunk_size, "block_size": block_size, "replicas": replicas}
        self.hb_timeout, self.lease_seconds = heartbeat_timeout, lease_seconds
        self.lock = threading.RLock()
        self.files = {}          # path -> [chunk handle, ...]
        self.chunks = {}         # handle -> {"version": int, "locs": set(addr tuples), "primary": addr|None,
        #                                    "lease": float, "length": int,
        #                                    "acked": {addr: version the replica confirmed}}
        self.servers = {}        # addr tuple -> {"last": float, "chunks": set(handle)}
        self.log_path = os.path.join(root, "master.oplog")
        self._replay()
        self.log = open(self.log_path, "a", buffering=1)
        self.server = protocol.Server(self._dispatch, host, port)
        self.addr = self.server.addr
        self._stop = threading.Event()
        self.started = time.time()       # after a (re)start, locations are unknown until heartbeats arrive: hold off re-replication
        self._maint = threading.Thread(target=self._maintenance, args=(maintenance_interval,), daemon=True)

    # ------------------------------------------------------------------ lifecycle
    def start(self):
        self.server.start()
        self._maint.start()
        return self

    def stop(self):
        self._stop.set()
        self.server.stop()
        self.log.close()

    # ------------------------------------------------------------------ operation log
    def _log(self, **rec):
        self.log.write(json.dumps(rec) + "\n")

