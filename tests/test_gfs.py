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

    def test_overwrite_in_the_middle(self):
        self.cl.create("/o")
        base = pattern(6000, 2)
        self.cl.write("/o", 0, base)
        patch = b"PATCH" * 100
        self.cl.write("/o", 4090, patch)                                       # crosses a chunk boundary
        expect = bytearray(base); expect[4090:4090 + len(patch)] = patch
        self.assertEqual(self.cl.read_all("/o"), bytes(expect))

    def test_every_chunk_has_three_identical_replicas(self):
        self.cl.create("/r")
        data = pattern(4096 * 2 + 500, 3)
        self.cl.write("/r", 0, data)
        counts = self.c.replica_counts()
        self.assertEqual(len(counts), 3)
        self.assertTrue(all(n == 3 for n in counts.values()), counts)
        for h in counts:                                                         # byte-identical on disk
            blobs = {blob(p) for p in glob.glob(os.path.join(self.c.root, "cs*", h + ".chunk"))}
            self.assertEqual(len(blobs), 1, "replicas differ")

    def test_master_stores_only_metadata_not_data(self):
        self.cl.create("/m")
        self.cl.write("/m", 0, pattern(9000, 4))
        master_files = os.listdir(os.path.join(self.c.root, "master"))
        self.assertEqual(master_files, ["master.oplog"])                       # no chunk data lives on the master

    def test_record_append_concurrent_writers(self):
        self.cl.create("/log")
        n_threads, per_thread = 6, 25
        offsets, errors = {}, []

        def worker(t):
            cl = self.c.client()
            for i in range(per_thread):
                rec = struct.pack(">HH", t, i) + pattern(30 + (i * 7) % 50, t * 1000 + i)
                try:
                    offsets[(t, i)] = (cl.append("/log", struct.pack(">I", len(rec)) + rec), rec)
                except GFSError as e:
                    errors.append(e)
        ths = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
        [t.start() for t in ths]; [t.join() for t in ths]
        self.assertEqual(errors, [])
        # every record readable exactly at the offset GFS returned; no two records overlap
        spans = sorted((off, off + 4 + len(rec)) for off, rec in offsets.values())
        for (a0, a1), (b0, b1) in zip(spans, spans[1:]):
            self.assertLessEqual(a1, b0, "records overlap")
        for (t, i), (off, rec) in offsets.items():
            got = self.cl.read("/log", off, 4 + len(rec))
            self.assertEqual(got, struct.pack(">I", len(rec)) + rec)
        self.assertEqual(len(offsets), n_threads * per_thread)

    def test_survives_chunkserver_failure_and_rereplicates(self):
        self.cl.create("/f")
        data = pattern(4096 * 3, 5)
        self.cl.write("/f", 0, data)
        victim = self.c.servers.index(next(s for s in self.c.servers if any(
            os.path.exists(os.path.join(s.root, h + ".chunk")) for h in self.c.replica_counts())))
        self.c.kill(victim)
        self.assertEqual(self.cl.read_all("/f"), data)                         # reads fall back to surviving replicas
        ok = self.c.wait_until(lambda: all(n == 3 for n in self.c.replica_counts().values()), timeout=15)
        self.assertTrue(ok, f"did not restore replication: {self.c.replica_counts()}")
        self.assertEqual(self.c.client().read_all("/f"), data)

    def test_writes_continue_after_primary_dies(self):
        self.cl.create("/p")
        self.cl.write("/p", 0, pattern(1000, 6))
        info = self.cl._chunk("/p", 0, mutate=True)
        primary = tuple(info["primary"])
        self.c.kill(next(i for i, s in enumerate(self.c.servers) if tuple(s.addr) == primary))
        self.assertTrue(self.c.wait_until(lambda: primary not in self.c.master._live(), timeout=10))
        self.cl._cache.clear()
        self.cl.write("/p", 1000, b"after-failover")                            # master grants a lease to a surviving replica
        self.assertEqual(self.cl.read("/p", 1000, 14), b"after-failover")

    def test_corrupt_replica_detected_and_repaired(self):
        self.cl.create("/c")
        data = pattern(4096, 7)
        self.cl.write("/c", 0, data)
        h = next(iter(self.c.replica_counts()))
        victim = sorted(glob.glob(os.path.join(self.c.root, "cs*", h + ".chunk")))[0]
        with open(victim, "r+b") as f:                                         # flip bytes in one replica behind GFS's back
            f.seek(100); f.write(b"\xde\xad\xbe\xef")
        for _ in range(6):                                                     # any replica may be tried first; all reads must be correct
            self.assertEqual(self.c.client().read("/c", 0, 4096), data)
        ok = self.c.wait_until(lambda: self.c.replica_counts().get(h) == 3 and
                               len({blob(p) for p in glob.glob(os.path.join(self.c.root, "cs*", h + ".chunk"))}) == 1, timeout=15)
        self.assertTrue(ok, "corrupt replica was not repaired")

