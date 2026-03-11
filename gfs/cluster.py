"""In-process cluster for tests and demos: one master and N chunkservers, each with its own TCP port
and its own directory (real sockets, real files); "crashing" a server just stops it."""
import os
import shutil
import tempfile
import time

from .chunkserver import ChunkServer
from .client import GFSClient
from .master import Master


