"""In-process cluster for tests and demos: one master and N chunkservers, each with its own TCP port
and its own directory (real sockets, real files); "crashing" a server just stops it."""
import os
import shutil
import tempfile
import time

from .chunkserver import ChunkServer
from .client import GFSClient
from .master import Master


class LocalCluster:
    def __init__(self, n_servers=5, chunk_size=4096, block_size=512, replicas=3, root=None,
                 heartbeat_timeout=1.2, heartbeat_interval=0.2, maintenance_interval=0.3, scrub_interval=0.4):
        self.root = root or tempfile.mkdtemp(prefix="gfs-")
        self._own_root = root is None
        self.params = dict(chunk_size=chunk_size, block_size=block_size, replicas=replicas,
                           heartbeat_timeout=heartbeat_timeout, maintenance_interval=maintenance_interval)
        self.hb_interval, self.scrub_interval = heartbeat_interval, scrub_interval
        self.master = Master(os.path.join(self.root, "master"), **self.params).start()
        self.servers = []
        for i in range(n_servers):
            self.add_server()
        self.wait_ready(n_servers)

    def add_server(self):
        i = len(self.servers)
        s = ChunkServer(os.path.join(self.root, f"cs{i}"), self.master.addr, self.hb_interval, self.scrub_interval).start()
        self.servers.append(s)
        return s

    def wait_ready(self, n, timeout=5.0):
        t0 = time.time()
        while time.time() - t0 < timeout:
            with self.master.lock:
                if len(self.master._live()) >= n: return
            time.sleep(0.05)
        raise TimeoutError("chunkservers did not register")

    def client(self):
        return GFSClient(self.master.addr)

    def kill(self, i):
        self.servers[i].stop()

    def restart_master(self):
        addr = self.master.addr
        self.master.stop()
        self.master = Master(os.path.join(self.root, "master"), port=addr[1], **self.params).start()

    def replica_counts(self):
        """handle -> number of live replicas according to the master"""
        with self.master.lock:
            return {h: len(self.master._live_locs(h)) for h in self.master.chunks}

    def wait_until(self, cond, timeout=10.0, step=0.1):
        t0 = time.time()
        while time.time() - t0 < timeout:
            if cond(): return True
            time.sleep(step)
        return False

    def close(self):
        for s in self.servers:
            if s.alive: s.stop()
        self.master.stop()
        if self._own_root: shutil.rmtree(self.root, ignore_errors=True)
