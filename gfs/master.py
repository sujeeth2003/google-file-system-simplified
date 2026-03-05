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

    def _replay(self):
        if not os.path.exists(self.log_path):
            return
        with open(self.log_path) as f:
            for line in f:
                r = json.loads(line)
                op = r["op"]
                if op == "create": self.files.setdefault(r["path"], [])
                elif op == "delete":
                    for h in self.files.pop(r["path"], []): self.chunks.pop(h, None)
                elif op == "add_chunk":
                    self.files[r["path"]].append(r["handle"])
                    self.chunks[r["handle"]] = {"version": r["version"], "locs": set(), "primary": None, "lease": 0.0, "length": 0, "acked": {}}
                elif op == "version": self.chunks[r["handle"]]["version"] = r["version"]

    # ------------------------------------------------------------------ helpers
    def _live(self):
        now = time.time()
        return [a for a, s in self.servers.items() if now - s["last"] < self.hb_timeout]

    def _live_locs(self, handle):
        live = set(self._live())
        return [a for a in self.chunks[handle]["locs"] if a in live]

    def _rpc(self, addr, msg, payload=b"", timeout=3.0):
        try:
            resp, out = protocol.call(addr, msg, payload, timeout)
            return resp, out
        except OSError:
            return {"ok": False, "error": "unreachable"}, b""

    def _place(self, n, exclude=()):
        """Pick n live servers, preferring the ones holding the fewest chunks."""
        cands = [a for a in self._live() if a not in exclude]
        random.shuffle(cands)
        cands.sort(key=lambda a: len(self.servers[a]["chunks"]))
        return cands[:n]

    # ------------------------------------------------------------------ RPC dispatch
    def _dispatch(self, msg, payload):
        fn = getattr(self, "rpc_" + msg["op"], None)
        if fn is None:
            return {"ok": False, "error": f"unknown op {msg['op']}"}, b""
        with self.lock:
            return fn(msg), b""

    def rpc_config(self, m):
        return {"ok": True, **self.cfg}

    def rpc_create(self, m):
        if m["path"] in self.files:
            return {"ok": False, "error": "exists"}
        self.files[m["path"]] = []
        self._log(op="create", path=m["path"])
        return {"ok": True}

    def rpc_delete(self, m):
        handles = self.files.pop(m["path"], None)
        if handles is None:
            return {"ok": False, "error": "no such file"}
        self._log(op="delete", path=m["path"])
        for h in handles:            # lazy garbage collection: chunkservers drop unknown chunks on their next heartbeat
            self.chunks.pop(h, None)
        return {"ok": True}

    def rpc_list(self, m):
        return {"ok": True, "files": sorted(p for p in self.files if p.startswith(m.get("prefix", "")))}

    def rpc_stat(self, m):
        if m["path"] not in self.files:
            return {"ok": False, "error": "no such file"}
        hs = self.files[m["path"]]
        total = 0
        for h in hs:                     # ask a live replica for the current length (heartbeat data can lag)
            n = self.chunks[h]["length"]
            for a in self._live_locs(h):
                r, _ = self._rpc(a, {"op": "length", "handle": h})
                if r.get("ok"): n = r["length"]; break
            total += n
        return {"ok": True, "chunks": len(hs), "length": total}

    def rpc_heartbeat(self, m):
        addr = tuple(m["addr"])
        info = self.servers.setdefault(addr, {"last": 0.0, "chunks": set()})
        info["last"] = time.time()
        info["chunks"] = set()
        stale = []
        for h, (ver, length) in m["chunks"].items():
            c = self.chunks.get(h)
            # A heartbeat is a snapshot: one taken just before a lease grant bumped the version can arrive after the
            # bump. A server that already acknowledged the current version is therefore NOT stale.
            behind = c is not None and ver < c["version"] and c["acked"].get(addr) != c["version"]
            if c is None or behind:                  # unknown (deleted) or stale replica: tell the server to drop it
                stale.append(h)
                if c is not None: c["locs"].discard(addr)
                continue
            c["locs"].add(addr)
            c["length"] = max(c["length"], length)
            info["chunks"].add(h)
        return {"ok": True, "delete": stale}

    def rpc_report_corrupt(self, m):
        h, addr = m["handle"], tuple(m["addr"])
        c = self.chunks.get(h)
        if c:
            c["locs"].discard(addr)
            if c["primary"] == addr: c["primary"], c["lease"] = None, 0.0
        if addr in self.servers: self.servers[addr]["chunks"].discard(h)
        self._rpc(addr, {"op": "delete_chunk", "handle": h})
        return {"ok": True}

    def _allocate_chunk(self, path):
        servers = self._place(self.cfg["replicas"])
        if not servers:
            raise RuntimeError("no live chunkservers")
        h = uuid.uuid4().hex[:16]
        self.chunks[h] = {"version": 1, "locs": set(), "primary": None, "lease": 0.0, "length": 0, "acked": {}}
        self.files[path].append(h)
        self._log(op="add_chunk", path=path, handle=h, version=1)
        for a in servers:
            r, _ = self._rpc(a, {"op": "create_chunk", "handle": h, "version": 1})
            if r.get("ok"):
                self.chunks[h]["locs"].add(a); self.servers[a]["chunks"].add(h)
        return h

    def _grant_lease(self, h):
        """Make sure chunk h has a valid primary. Granting a new lease bumps the chunk version so any
        replica that was unreachable (and therefore misses the bump) is recognised as stale later."""
        c = self.chunks[h]
        now = time.time()
        live = self._live_locs(h)
        if c["primary"] in live and c["lease"] > now:
            return
        if not live:
            raise RuntimeError("chunk has no live replica")
        c["version"] += 1
        self._log(op="version", handle=h, version=c["version"])
        ok = []
        for a in live:
            r, _ = self._rpc(a, {"op": "set_version", "handle": h, "version": c["version"]})
            if r.get("ok"): ok.append(a)
        if not ok:
            raise RuntimeError("no replica accepted the new version")
        c["acked"] = {a: c["version"] for a in ok}
        c["locs"] = set(ok) | (c["locs"] - set(live))
        c["primary"], c["lease"] = ok[0], now + self.lease_seconds

