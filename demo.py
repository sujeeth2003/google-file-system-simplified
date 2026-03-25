"""Demo: write data, kill a chunkserver, keep reading, and watch the master restore three replicas."""
import time

from gfs import LocalCluster

c = LocalCluster(n_servers=5, chunk_size=4096, block_size=512, replicas=3)
try:
    cl = c.client()
    cl.create("/demo")
    payload = bytes(range(256)) * 60                                    # 15 KiB -> 4 chunks
    cl.write("/demo", 0, payload)
    print("chunks and live replicas:", list(c.replica_counts().values()))

    victim = next(i for i, s in enumerate(c.servers) if any(f.endswith(".chunk") for f in __import__("os").listdir(s.root)))
    print(f"killing chunkserver {victim}")
    t0 = time.time()
    c.kill(victim)
    assert cl.read_all("/demo") == payload
    print("read after crash: OK (served from surviving replicas)")

    dead = tuple(c.servers[victim].addr)
    c.wait_until(lambda: dead not in c.master._live(), timeout=20)          # heartbeats stop -> master declares it dead
    print(f"master noticed the failure {time.time() - t0:.1f}s after the crash; replicas now {list(c.replica_counts().values())}")
    c.wait_until(lambda: all(n == 3 for n in c.replica_counts().values()), timeout=20)
    print(f"replication restored to {list(c.replica_counts().values())} {time.time() - t0:.1f}s after the crash")

    offsets = [cl.append("/demo", f"record-{i}".encode()) for i in range(3)]
    print("record appends at offsets", offsets)
finally:
    c.close()
