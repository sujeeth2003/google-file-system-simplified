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

