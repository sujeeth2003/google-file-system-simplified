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


