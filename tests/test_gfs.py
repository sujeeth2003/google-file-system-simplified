import glob
import os
import random
import struct
import sys
import threading
import unittest
import zlib

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from gfs import GFSError, LocalCluster  # noqa: E402


def blob(path):
    with open(path, "rb") as f:
        return f.read()


def pattern(n, seed=0):
    r = random.Random(seed)
    return bytes(r.getrandbits(8) for _ in range(n))


class GFSTests(unittest.TestCase):
    def setUp(self):
        self.c = LocalCluster(n_servers=5, chunk_size=4096, block_size=512, replicas=3)
        self.cl = self.c.client()

    def tearDown(self):
        self.c.close()

    def test_write_read_across_chunk_boundaries(self):
        self.cl.create("/a")
        data = pattern(4096 * 3 + 1234, 1)          # spans 4 chunks
        self.cl.write("/a", 0, data)
        self.assertEqual(self.cl.stat("/a"), len(data))
        self.assertEqual(self.cl.read("/a", 0, len(data)), data)
        self.assertEqual(self.cl.read("/a", 4000, 300), data[4000:4300])     # straddles chunk 0/1
        self.assertEqual(self.cl.read_all("/a"), data)

