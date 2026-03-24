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

