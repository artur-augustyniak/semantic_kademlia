#!/usr/bin/env python3

import math
import threading
import hashlib
import socket
import random
import struct


class Network:
    def __init__(self):
        self._nodes = {}
        self._lock = threading.RLock()

    def register(self, node):
        with self._lock:
            self._nodes[node.node_id] = node

    def unregister(self, node):
        with self._lock:
            del self._nodes[node.node_id]

    def get(self, node_id):
        with self._lock:
            return self._nodes.get(node_id)

    def keys(self):
        with self._lock:
            return list(self._nodes.keys())

    def values(self):
        with self._lock:
            return list(self._nodes.values())

    def live_nodes(self):
        with self._lock:
            return [n for n in self._nodes.values() if not n.stop_event.is_set()]

    def send(self, target_id, msg):
        with self._lock:
            target = self._nodes.get(target_id)
        if target is None or target.stop_event.is_set():
            return False
        target.inbox.put(msg)
        return True


def hash_to_ip(s):
    h = hashlib.sha256(s.encode()).digest()
    first4 = h[:4]
    return socket.inet_ntoa(first4)


def str_abbr_hash(s):
    return hashlib.sha256(s.encode()).hexdigest()[:8]


def hash_ngram(ngram):
    data = f"{ngram}".encode("utf-8")
    digest = hashlib.sha256(data).digest()
    return int.from_bytes(digest[:4], byteorder="little", signed=False)


def countsketch_embedding(text, dim):
    ngrams = (2, 3, 4)
    text = text.lower().strip()
    if not text:
        return [0.0] * dim

    t = "<" + text + ">"
    vec = [0.0] * dim
    for n in ngrams:
        if len(t) < n:
            continue
        for i in range(len(t) - n + 1):
            ng = t[i : i + n]
            h = hash_ngram(ng)
            idx = h % dim
            sign = 1.0 if (h & (1 << 31)) == 0 else -1.0
            vec[idx] += sign

    norm = math.sqrt(sum(x * x for x in vec))
    if norm > 0:
        vec = [x / norm for x in vec]
    return vec


def vec_to_hex(vec):
    items = list(vec)
    packed = struct.pack(f"!{len(items)}d", *items)
    return packed.hex()


def hex_to_vec(hex_str):
    packed = bytes.fromhex(hex_str)
    n = len(packed) // 8
    vec = struct.unpack(f"!{n}d", packed)
    return list(vec)


def embed_vec_hex(text, dim):
    return vec_to_hex(countsketch_embedding(text, dim))


def random_unit_vec(dim):
    xs = [random.gauss(0, 1) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in xs))
    return [x / norm for x in xs]


def random_unit_vec_hex(dim):
    return vec_to_hex(random_unit_vec(dim))


def cosine_sim(u_hex, v_hex):
    u = hex_to_vec(u_hex)
    v = hex_to_vec(v_hex)
    dot = sum(a * b for a, b in zip(u, v))
    nu = math.sqrt(sum(a * a for a in u))
    nv = math.sqrt(sum(b * b for b in v))
    if nu == 0 or nv == 0:
        return 0.0
    return dot / (nu * nv)


def cosine_dist(u_hex, v_hex):
    return 1.0 - cosine_sim(u_hex, v_hex)
