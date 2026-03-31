# Simplified Google File System (Python)

A working, small-scale implementation of the ideas in *The Google File System* (Ghemawat, Gobioff, Leung, SOSP 2003): one **master**, several **chunkservers**, a **client library**, real TCP sockets between them and real files on disk. Built to understand the design, not to be production storage.

**What I learned so far:** how the metadata path and the data path are split so the master never becomes the bottleneck. Clients ask the master *where* a chunk lives, cache the answer, and then move all bytes directly to and from chunkservers.

```
              +----------+   metadata only (paths, chunk handles, locations, leases)
  client ---> |  master  | <--- heartbeats (chunk inventory) --- chunkservers
     |        +----------+
     |  data (push to all replicas, commit via primary)
     +-----------------------------> chunkserver x3 replicas per chunk
```

## What is implemented
| Paper feature | Here |
|---|---|
| Single master, metadata in memory | `gfs/master.py`: namespace, file -> chunk handles, chunk -> replica locations |
| Operation log for durability | Namespace and chunk versions are appended to `master.oplog` and replayed on restart. **Chunk locations are not logged**; they are rebuilt from chunkserver heartbeats, as in the paper |
| Large fixed-size chunks, replicated x3 | `chunk_size` configurable (64 MiB default, 4 KiB in tests); placement on the least-loaded live servers |
| Leases and version numbers | Master grants a primary a lease and bumps the chunk version; replicas that miss a bump are detected as stale and garbage-collected |
| Decoupled data and control flow | Client pushes data to every replica, then sends one commit request to the primary, which orders and forwards it (`gfs/client.py`, `gfs/chunkserver.py`) |
| Record append | Atomic append at an offset GFS chooses; pads the chunk and retries on the next one if the record does not fit; returns the offset |
| Chunk checksums | CRC32 per block, verified on every read; background scrubbing finds corruption in chunks nobody reads |
| Re-replication | Dead server -> master restores the replication factor by cloning from a healthy replica; over-replicated chunks are trimmed |
| Lazy garbage collection | Deleted files' chunks are dropped when servers next heartbeat |
| Master restart | Grace period so it does not panic-replicate before locations are known |

## Try it
```bash
python -m unittest discover -s tests -v      # ~40 s, starts a real 5-server cluster per test
python demo.py                               # write, kill a server, keep reading, watch re-replication
```
```python
from gfs import LocalCluster
c = LocalCluster(n_servers=5); cl = c.client()
cl.create("/logs/a"); off = cl.append("/logs/a", b"hello"); print(cl.read("/logs/a", off, 5))
```

## Tests (all pass; 10 tests)
Cross-chunk write/read, overwrite across a boundary, three byte-identical replicas per chunk, master stores no file data, 6 threads x 25 concurrent record appends (no overlap, each readable at its returned offset), reads survive a chunkserver crash **and** the replication factor is restored, writes continue after the primary dies, a silently corrupted replica is detected and repaired, master restart recovers the namespace, deleted files are garbage collected.

Writing these tests exposed four real bugs that are now fixed: a heartbeat snapshot racing a version bump made the master delete a healthy replica; `IOError` (== `OSError`) treated as checksum failure; `length` observed a replica mid-copy; the master over-replicated right after a restart.

## Not implemented (honest scope)
Multi-master / shadow masters and automatic master failover; snapshots; hierarchical directory locking (the namespace is a flat path map); rack-aware placement; chunk-server-to-chunkserver data pipelining (the client pushes to each replica directly); lease extension via heartbeat; master checkpoints (only an unbounded operation log). No authentication. It is a teaching implementation, not a storage system.
