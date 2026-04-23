# Semantic Kademlia

Repository accompanying the ["Semantic Kademlia"](article/semantic_kademlia.pdf) article. 
The repository demonstrates a simulation of a Kademlia-like network that operates in an embedding space instead of a discrete XOR keyspace.

Instead of node and key identifiers being hashes, they are vectors (embeddings), and routing is guided by cosine similarity. 

The project uses only the Python standard library (no external dependencies). 
It was tested with Python 3.12, but does not rely on any version-specific features.

## Repository contents

- `main.py` - simulation driver (network setup, seeding, queries)
- `node.py` - node logic (routing, message handling, replication)
- `tooling.py` - simulated transport layer, embedding (CountSketch) and helpers
- `trials/` - example runs:
  - stable network runs
  - runs with increased probability of node disappearance

## Running

```bash
python3 main.py
```

## Generate article
```bash
cd ./article
pdflatex semantic_kademlia.tex
```