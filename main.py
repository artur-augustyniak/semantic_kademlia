#!/usr/bin/env python3
import logging
import random
import threading
import time
import uuid
from tooling import Network
from tooling import embed_vec_hex
from tooling import str_abbr_hash
from tooling import hash_to_ip
from tooling import random_unit_vec_hex
from node import Node

logging.basicConfig(
    level=logging.INFO,
    # level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("semkad")


SIM_QUERY_INTERVAL = 5.0
SIM_NETWORK_SIZE = 70
MAX_NODE_BOOTSTRAP_NODES = 2
EMBED_DIM = 512
CHAOS_INTERVAL = 8


def seed_demo_data(network, embed_dim):
    samples = [
        "A distributed hash table stores values under unique keys across a decentralized overlay network.",
        "An epidemic gossip protocol spreads updates between peers through repeated local exchanges.",
        "A neural embedding encodes sentences into vectors and compares them using cosine similarity.",
        "A count sketch compresses character ngrams into a fixed size vector using hashed signed buckets.",
        "Breadth first search explores a graph by visiting vertices level by level from a starting node.",
        "A least recently used cache evicts stale entries when memory capacity is exceeded.",
        "Mutex locking ensures safe access to shared state and prevents race conditions in multithreaded code.",
        "An inverted index maps terms to posting lists for efficient full text document retrieval.",
        "A knowledge graph represents facts as subject predicate object triples linking entities.",
        "A bloom filter answers membership queries with possible false positives using a compact bit array.",
    ]
    nodes = network.values()
    initiators = random.sample(nodes, k=min(len(samples), len(nodes)))

    for text, initiator in zip(samples, initiators):

        key = embed_vec_hex(text, embed_dim)
        logger.info(
            "seeding data via node=%s (embedding_hash=%s text=%r)",
            initiator.short_id,
            str_abbr_hash(key),
            text,
        )
        initiator.post(
            {
                "type": "STORE",
                "key": key,
                "value": text,
            }
        )


def vectorize_queries(embed_dim):
    queries = [
        "how does a distributed hash table store values by key",
        "decentralized overlay lookup of values using a hash table",

        "how do gossip protocols spread updates between peers",
        "epidemic gossip message dissemination across neighboring nodes",

        "compare sentences using vector embeddings and cosine similarity",
        "how are texts represented in a neural embedding space",

        "how does count sketch compress character ngrams into buckets",
        "signed hashing of text features into a fixed size vector",

        "graph traversal using breadth first search by levels",
        "visit nodes layer by layer starting from a root vertex",

        "least recently used cache eviction under memory pressure",
        "evicts stale entries when cache capacity is exceeded",

        "prevent race conditions using mutex locking",
        "synchronize threads accessing shared state safely",

        "how does an inverted index support document retrieval",
        "posting lists mapping terms to documents in search systems",

        "represent facts as subject predicate object triples",
        "entity linking encoded in a knowledge graph",

        "probabilistic membership test with bloom filter false positives",
        "compact bit array structure for approximate set membership",
    ]
    query_keys = [embed_vec_hex(q, embed_dim) for q in queries]
    keyed_queries = list(zip(queries, query_keys))

    for q, qk in keyed_queries:
        logger.info(
            "predefined query (embedding_hash=%s text=%r)", str_abbr_hash(qk), q
        )
    return keyed_queries


def init_nodes(network):
    logger.info(
        "init nodes=%d embed_dim=%d bootstrap max_degree=%d",
        SIM_NETWORK_SIZE,
        EMBED_DIM,
        MAX_NODE_BOOTSTRAP_NODES,
    )
    for _ in range(SIM_NETWORK_SIZE):
        node_id = random_unit_vec_hex(EMBED_DIM)
        while network.get(node_id) is not None:
            node_id = random_unit_vec_hex(EMBED_DIM)
        network.register(Node(EMBED_DIM, node_id, network))

    all_ids = network.keys()
    nodes = network.values()

    for node in nodes:
        threading.Thread(target=node.run, daemon=True).start()

    for node in nodes:
        bootstrap = random.sample(all_ids, min(MAX_NODE_BOOTSTRAP_NODES, len(all_ids)))
        for peer_nid in bootstrap:
            if node.node_id != peer_nid:
                node.post({"type": "ADD_NODE", "node_id": peer_nid})
        node.post({"type": "BOOTSTRAP"})


def chaos_monkey(network):
    while True:
        time.sleep(CHAOS_INTERVAL)
        if random.random() < 0.4:
            live_nodes = network.live_nodes()
            if not live_nodes:
                return
            target = random.choice(live_nodes)
            logger.warning("[CHAOS] killing node=%s", target.short_id)
            target.stop()
            network.unregister(target)

        if random.random() < 0.4:
            live_nodes = network.live_nodes()
            if not live_nodes:
                return
            peer = random.choice(live_nodes)
            node_id = random_unit_vec_hex(EMBED_DIM)
            while network.get(node_id) is not None:
                node_id = random_unit_vec_hex(EMBED_DIM)
            new_node = Node(EMBED_DIM, node_id, network)
            network.register(new_node)

            threading.Thread(target=new_node.run, daemon=True).start()
            new_node.post({"type": "ADD_NODE", "node_id": peer.node_id})
            new_node.post({"type": "BOOTSTRAP"})
            logger.warning("[CHAOS] created new node=%s", new_node.short_id)


def run_query_loop(network, keyed_queries):
    # live_nodes = network.live_nodes()
    # if not live_nodes:
    #     return
    # requester = random.choice(live_nodes)
    while True:
        live_nodes = network.live_nodes()
        if not live_nodes:
            continue
        requester = random.choice(live_nodes)

        print("#" * 80)
        # for pair in keyed_queries:
        #     query_text, vector_hex = pair

        #     qid = str(uuid.uuid4())
        #     requester.post(
        #         {
        #             "type": "SEARCH",
        #             "key": vector_hex,
        #             "query_id": qid,
        #             "text" : query_text
        #         }
        #     )

        #     # for r in sorted(requester.routing_table):
        #     #     print(hash_to_ip(r))
        #     #     print("leeeeeeen", )
        #     logger.info(
        #         "node=%s [QUERY %s - %s] key=%s rtSize=%d rtHAsh=%s",
        #         requester.short_id,
        #         qid[:8],
        #         query_text[:80],
        #         str_abbr_hash(vector_hex),
        #         len(requester.routing_table),
        #         str_abbr_hash(str(sorted(requester.routing_table)))
        #     )




        query_text, vector_hex = random.choice(keyed_queries)

        qid = str(uuid.uuid4())
        requester.post(
            {
                "type": "SEARCH",
                "key": vector_hex,
                "query_id": qid,
            }
        )
        logger.info(
            "node=%s [QUERY %s - %s] key=%s",
            requester.short_id,
            qid[:8],
            query_text[:80],
            str_abbr_hash(vector_hex)
        )
        
        time.sleep(SIM_QUERY_INTERVAL)
        # print("#" * 80)


if __name__ == "__main__":
    logger.info("starting semantic kademlia simulation")
    keyed_queries = vectorize_queries(EMBED_DIM)
    network = Network()
    init_nodes(network)
    
    seed_demo_data(network, EMBED_DIM)
    
    # threading.Thread(target=chaos_monkey, args=(network,), daemon=True).start()
    run_query_loop(network, keyed_queries)
