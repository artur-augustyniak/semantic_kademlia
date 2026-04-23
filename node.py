#!/usr/bin/env python3
import threading
import time
import random
from queue import Empty, Queue
from tooling import hash_to_ip
from tooling import cosine_dist
from tooling import cosine_sim
from tooling import str_abbr_hash
from tooling import random_unit_vec_hex
import logging

logger = logging.getLogger("semkad")

MAX_ROUTING_TABLE_SIZE = 20
K = 2
ALPHA = 3
LOOKUP_STEP_DELAY = 0.08
FIND_VALUE_TTL = MAX_ROUTING_TABLE_SIZE
FIND_NODES_TTL = 3
DATA_TTL = 60.0
REFRESH_INTERVAL = DATA_TTL / 2.1
QUEUE_MSG_SOFT_TIMEOUT = 0.1
RESPONSE_DEDUP_TTL = 30.0
QUERY_DEDUP_TTL = 20.0
QUERY_THRESHOLD = 0.4


class Node:
    def __init__(
        self,
        embed_dim: int,
        node_id: str,
        network,
    ):
        self.embed_dim = embed_dim
        self.node_id = node_id
        self.last_query_key = node_id
        self.short_id = hash_to_ip(node_id)
        self.network = network
        self.routing_table = set()
        self.inbox = Queue()
        self.data_store = {}
        self.seen_find_value_queries = {}
        self.seen_find_value_responses = {}
        self._idle_counter = 0
        self.stop_event = threading.Event()
        self._threads = []

    def post(self, msg):
        if not self.stop_event.is_set():
            self.inbox.put(msg)

    def stop(self):
        self.stop_event.set()

    def _add_node(self, other_id):
        if other_id == self.node_id:
            return
        if other_id in self.routing_table:
            return
        other = self.network.get(other_id)
        if other is None or other.stop_event.is_set():
            return
        self.routing_table.add(other_id)
        if len(self.routing_table) > MAX_ROUTING_TABLE_SIZE:
            kept = sorted(
                self.routing_table,
                key=lambda nid: cosine_dist(nid, self.last_query_key),
            )[:MAX_ROUTING_TABLE_SIZE]
            self.routing_table = set(kept)

    def _prune_dead_routing_entries(self):
        alive = set()
        for nid in self.routing_table:
            peer = self.network.get(nid)
            if peer is not None and not peer.stop_event.is_set():
                alive.add(nid)
        self.routing_table = alive

    def _get_locally_closest_nodes(self, target_vec, k=K):
        self._prune_dead_routing_entries()
        no_items = min(len(self.routing_table), k)
        return sorted(
            self.routing_table,
            key=lambda nid: cosine_dist(nid, target_vec),
        )[:no_items]

    def _cleanup_seen_maps(self):
        now = time.time()
        self.seen_find_value_queries = {
            marker: ts
            for marker, ts in self.seen_find_value_queries.items()
            if now - ts < QUERY_DEDUP_TTL
        }
        self.seen_find_value_responses = {
            marker: ts
            for marker, ts in self.seen_find_value_responses.items()
            if now - ts < RESPONSE_DEDUP_TTL
        }

    def _best_local_match(self, query_vec):
        best_key = None
        best_item = None
        best_sim = 0.0
        for stored_vec, item in list(self.data_store.items()):
            sim = cosine_sim(stored_vec, query_vec)
            if sim >= QUERY_THRESHOLD and sim > best_sim:
                best_sim = sim
                best_key = stored_vec
                best_item = item
        return best_key, best_item, best_sim

    def _iterative_improve_routing_towards(self, target_id, originator, ttl):
        a_locally_best = set(self._get_locally_closest_nodes(target_id, ALPHA))
        for peer_node_id in a_locally_best:
            self.network.send(
                peer_node_id,
                {
                    "type": "GET_CLOSEST_ROUTES",
                    "from": self.node_id,
                    "target_id": target_id,
                    "originator": originator,
                    "ttl": ttl,
                },
            )
        self.stop_event.wait(LOOKUP_STEP_DELAY)

    def _store(self, key, value):
        if self.stop_event.is_set():
            return
        self.last_query_key = key
        self._iterative_improve_routing_towards(key, self.node_id, FIND_NODES_TTL)
        targets = self._get_locally_closest_nodes(key)
        if not targets:
            self.data_store[key] = {"value": value, "stored_at": time.time()}
            return
        for node_id in targets:
            self._add_node(node_id)
            self.network.send(
                node_id,
                {
                    "type": "LOCAL_STORE",
                    "from": self.node_id,
                    "key": key,
                    "value": value,
                },
            )

    def _find_value(self, key, query_id):
        if self.stop_event.is_set():
            return
        self.last_query_key = key
        best_key, best_item, best_sim = self._best_local_match(key)
        if best_item is not None and best_key is not None:
            logger.info(
                "node=%s [RESPL %s - %s] local.hit key=%s score=%.3f",
                self.short_id,
                query_id[:8],
                best_item["value"][:80],
                str_abbr_hash(best_key),
                best_sim,
            )
            return
        self._iterative_improve_routing_towards(key, self.node_id, FIND_NODES_TTL)
        targets = targets = self._get_locally_closest_nodes(key)
        for node_id in targets:
            self.network.send(
                node_id,
                {
                    "type": "FIND_VALUE",
                    "from": self.node_id,
                    "origin": self.node_id,
                    "key": key,
                    "query_id": query_id,
                    "visited": [self.node_id],
                    "curr_ttl": FIND_VALUE_TTL,
                    "max_ttl": FIND_VALUE_TTL,
                },
            )

    def _handle_message(self, msg):
        msg_type = msg["type"]
        #####################################################
        # api msgs
        #####################################################
        if msg_type == "ADD_NODE":
            self._add_node(msg["node_id"])
            return

        if msg_type == "BOOTSTRAP":
            self._iterative_improve_routing_towards(
                self.node_id, self.node_id, FIND_NODES_TTL
            )
            return

        if msg_type == "STORE":
            self._store(msg["key"], msg["value"])
            return

        if msg_type == "SEARCH":
            self._find_value(msg["key"], msg["query_id"])
            return

        #####################################################
        # internal msgs
        #####################################################
        sender_id = msg["from"]
        '''
            In Kademlia, this makes sense; in our case, it just injects noise into the current semantic routing.
            We use it only in special circumstances, i.e., when our routing table is empty due to a massive disappearance of nodes.
        '''
        if len(self.routing_table) == 0:
            logger.critical(
                "node=%s all.peers.dead unconditionally adding seen node=%s",
                self.short_id,
                hash_to_ip(sender_id)
                )

            self._add_node(sender_id)

        if msg_type == "GET_CLOSEST_ROUTES":
            target_id = msg["target_id"]
            originator = msg["originator"]
            visited = set(msg.get("visited", []))
            visited.add(self.node_id)
            ttl = msg["ttl"] - 1
            if ttl <= 0:
                logger.debug(
                    "node=%s route.lookup.ttl from=%s originator=%s TTL=%d/%d",
                    self.short_id,
                    hash_to_ip(sender_id),
                    hash_to_ip(originator),
                    ttl,
                    FIND_NODES_TTL,
                )
                return
            no_items = min(len(self.routing_table), K)
            closest = self._get_locally_closest_nodes(target_id)[:no_items]
            self.network.send(
                originator,
                {
                    "type": "CLOSEST_ROUTES_RESPONSE",
                    "from": self.node_id,
                    "nodes": closest,
                    "target_id": target_id,
                    "ttl": ttl,
                },
            )
            return

        if msg_type == "CLOSEST_ROUTES_RESPONSE":
            target_id = msg["target_id"]
            all_nodes = self._get_locally_closest_nodes(
                target_id, k=len(self.routing_table)
            )
            best_local_node = all_nodes[0] if all_nodes else None
            best_local_node_metric = (
                cosine_sim(best_local_node, target_id)
                if best_local_node is not None
                else -99999999999
            )
            for peer_nid in msg["nodes"]:
                if peer_nid != self.node_id:
                    new_node_metric = cosine_sim(peer_nid, target_id)
                    if new_node_metric > best_local_node_metric:
                        self._iterative_improve_routing_towards(
                            target_id, peer_nid, msg["ttl"]
                        )
                        self._add_node(peer_nid)
            return

        if msg_type == "LOCAL_STORE":
            key = msg["key"]
            value = msg["value"]
            self._add_node(sender_id)
            self.last_query_key = key
            self.data_store[key] = {"value": value, "stored_at": time.time()}
            return

        if msg_type == "FIND_VALUE":
            origin = msg.get("origin", sender_id)
            query_vec = msg["key"]
            query_id = msg["query_id"]
            curr_ttl = msg["curr_ttl"] - 1

            qmarker = (origin, query_id)

            if qmarker in self.seen_find_value_queries:
                logger.debug(
                    "node=%s [qDUP - %s] lookup.duplicate from=%s originator=%s key=%s TTL=%d/%d",
                    self.short_id,
                    msg["query_id"][:8],
                    hash_to_ip(sender_id),
                    hash_to_ip(origin),
                    str_abbr_hash(msg["key"]),
                    curr_ttl,
                    FIND_VALUE_TTL,
                )
                return
            self.seen_find_value_queries[qmarker] = time.time()

            if curr_ttl <= 0:
                logger.warning(
                    "node=%s [qTTL - %s] lookup.ttl from=%s originator=%s key=%s TTL=%d/%d",
                    self.short_id,
                    msg["query_id"][:8],
                    hash_to_ip(sender_id),
                    hash_to_ip(origin),
                    str_abbr_hash(msg["key"]),
                    curr_ttl,
                    FIND_VALUE_TTL,
                )
                return
            best_key, best_item, best_sim = self._best_local_match(query_vec)

            if best_item is not None and best_key is not None:
                self.last_query_key = best_key
                self.network.send(
                    origin,
                    {
                        "type": "LOCAL_FIND_VALUE_RESPONSE",
                        "from": self.node_id,
                        "origin": origin,
                        "key": best_key,
                        "query": query_vec,
                        "value": best_item["value"],
                        "query_id": query_id,
                        "score": best_sim,
                        "curr_ttl": curr_ttl,
                        "max_ttl": FIND_VALUE_TTL,
                    },
                )
                self.network.send(origin, {"type": "ADD_NODE", "node_id": self.node_id})
                return

            visited = set(msg.get("visited", []))
            visited.add(self.node_id)
            candidates = self._get_locally_closest_nodes(
                query_vec, len(self.routing_table)
            )
            chosen = [
                nid for nid in candidates if nid not in visited and nid != self.node_id
            ][:ALPHA]
            if not chosen:
                logger.warning(
                    "node=%s [LOOP  %s] random.route from=%s key=%s TTL=%d/%d",
                    self.short_id,
                    msg["query_id"][:8],
                    hash_to_ip(origin),
                    str_abbr_hash(msg["key"]),
                    curr_ttl,
                    FIND_VALUE_TTL,
                )
                self._prune_dead_routing_entries()
                if len(self.routing_table) == 0:
                    self._add_node(sender_id)
                    logger.critical(
                        "node=%s [DROP  %s] empty routing table - all peers dead",
                        self.short_id,
                        msg["query_id"][:8]
                    )
                    return 
                chosen = [random.choice(list(self.routing_table))]


            for nid in chosen:
                self.network.send(
                    nid,
                    {
                        "type": "FIND_VALUE",
                        "from": self.node_id,
                        "origin": origin,
                        "key": query_vec,
                        "query_id": query_id,
                        "visited": list(visited),
                        "curr_ttl": curr_ttl,
                        "max_ttl": FIND_VALUE_TTL,
                    },
                )
            return

        if msg_type == "LOCAL_FIND_VALUE_RESPONSE":
            self._add_node(sender_id)
            self.last_query_key = msg["key"]
            resp_marker = (msg["query_id"], msg["key"])
            if resp_marker in self.seen_find_value_responses:
                logger.debug(
                    "node=%s [rDUP - %s] response.duplicate from=%s key=%s TTL=%d/%d",
                    self.short_id,
                    msg["query_id"][:8],
                    hash_to_ip(sender_id),
                    str_abbr_hash(msg["key"]),
                    msg["curr_ttl"],
                    FIND_VALUE_TTL,
                )
                return
            self.seen_find_value_responses[resp_marker] = time.time()

            logger.info(
                "node=%s [RESP  %s - %s] lookup.hit from=%s key=%s score=%.3f TTL=%d/%d",
                self.short_id,
                msg["query_id"][:8],
                msg["value"][:80],
                hash_to_ip(sender_id),
                str_abbr_hash(msg["key"]),
                msg["score"],
                msg["curr_ttl"],
                msg["max_ttl"],
            )
            self._cleanup_seen_maps()
            return

        if msg_type == "LOCAL_CLEANUP":
            self._cleanup_seen_maps()
            return

        if msg_type == "LOCAL_REFRESH":
            random_id = random_unit_vec_hex(self.embed_dim)
            self._iterative_improve_routing_towards(
                random_id, self.node_id, FIND_NODES_TTL
            )
            return

        if msg_type == "LOCAL_REPUBLISH":
            now = time.time()
            expired_keys = [
                k
                for k, item in self.data_store.items()
                if now - item["stored_at"] > DATA_TTL
            ]
            for key in expired_keys:
                logger.warning(
                    "node=%s dropping overdue entry %s",
                    self.short_id,
                    str_abbr_hash(key),
                )
                del self.data_store[key]

            for key, item in list(self.data_store.items()):
                self._iterative_improve_routing_towards(
                    key, self.node_id, FIND_NODES_TTL
                )
                candidates = set(
                    self._get_locally_closest_nodes(
                        key, k=max(MAX_ROUTING_TABLE_SIZE, K)
                    )
                )
                candidates.add(self.node_id)
                closest = sorted(candidates, key=lambda nid: cosine_dist(nid, key))[:K]
                for node_id in closest:
                    self._add_node(node_id)
                    logger.info(
                        "node=%s republish key=%s to=%s",
                        self.short_id,
                        str_abbr_hash(key),
                        hash_to_ip(node_id),
                    )
                    if node_id == self.node_id:
                        self.data_store[key] = {
                            "value": item["value"],
                            "stored_at": time.time(),
                        }
                    else:
                        self.network.send(
                            node_id,
                            {
                                "type": "LOCAL_STORE",
                                "from": self.node_id,
                                "key": key,
                                "value": item["value"],
                            },
                        )
            return
        logger.critical("node=%s unknown_msg=%s", self.short_id, msg_type)

    def _cleanup_loop(self):
        while not self.stop_event.is_set():
            self.stop_event.wait(15.0)
            self.post({"type": "LOCAL_CLEANUP", "from": self.node_id})

    def _refresh_buckets(self):
        while not self.stop_event.is_set():
            delay = REFRESH_INTERVAL + random.uniform(1.0, 10.0)
            self.stop_event.wait(delay)
            self.post({"type": "LOCAL_REFRESH", "from": self.node_id})

    def _republish_data(self):
        while not self.stop_event.is_set():
            self.post({"type": "LOCAL_REPUBLISH", "from": self.node_id})
            self.stop_event.wait(DATA_TTL / 2.0)

    def run(self):
        self._threads = [
            threading.Thread(target=self._refresh_buckets, daemon=True),
            threading.Thread(target=self._republish_data, daemon=True),
            threading.Thread(target=self._cleanup_loop, daemon=True),
        ]
        for t in self._threads:
            t.start()

        while not self.stop_event.is_set():
            try:
                msg = self.inbox.get(timeout=QUEUE_MSG_SOFT_TIMEOUT)
                logger.debug(
                    "node=%s processing msg=%s",
                    self.short_id,
                    msg["type"],
                )
                self._handle_message(msg)
            except Empty:
                self._idle_counter += 1
                continue
            except Exception as e:
                logger.critical(
                    "node=%s crashed with error=%s",
                    self.short_id,
                    str(e),
                    exc_info=True,
                )
                self.stop()
                break

        logger.info("node=%s stopped", self.short_id)
