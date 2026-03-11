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

