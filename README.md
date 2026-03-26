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

