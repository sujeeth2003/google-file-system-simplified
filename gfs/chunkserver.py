"""GFS chunkserver: stores chunks as ordinary files, serves reads, applies mutations in a primary-assigned order.

Write path (paper figure 2, simplified):
  1. client pushes the DATA to every replica (buffered here under a data id, nothing applied yet)
  2. client sends the mutation request to the PRIMARY
  3. the primary picks an offset/serial order, applies the buffered data, and forwards the same
     instruction to the secondaries, which apply it in that same order
  4. primary replies once every replica has applied it
Data flow (step 1) is decoupled from control flow (steps 2-4).

Integrity: every chunk keeps a CRC32 per block; reads verify the blocks they touch and a mismatch is reported to the master,
which then re-replicates the chunk from a healthy replica.
"""
import json
import os
import threading
import time
import zlib

