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

