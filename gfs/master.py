"""GFS master: namespace + chunk metadata + chunk placement, leases, re-replication.

What the master stores (as in the paper):
  * file namespace and file -> ordered list of chunk handles   (persisted in an operation log)
  * chunk version numbers                                       (persisted; used to detect stale replicas)
  * chunk LOCATIONS                                             (NOT persisted: rebuilt from chunkserver heartbeats)
Data never flows through the master, which is what keeps it from becoming the bottleneck: clients ask it
"which chunkservers hold chunk N of file F?", cache the answer, and then talk to chunkservers directly.
"""
import json
import os
import random
import threading
import time
import uuid

