# otter

Implementation of the Raft consensus algorithm in Go.

Named after the otter — a group of otters is called a *raft*. These animals float together, holding on to each other to stay in sync, much like Raft nodes replicate logs and elect leaders to maintain consensus.

Part of a trilogy of distributed systems projects:
1. **[walrus](https://github.com/arbhalerao/walrus)** — single-node persistent KV store with WAL
2. **[meerkat](https://github.com/arbhalerao/meerkat)** — distributed KV with consistent hashing and replication
3. **[otter](https://github.com/arbhalerao/otter)** — Raft consensus protocol from scratch _(you are here)_

## Build

```
make build
```

## Running a Cluster

Each node runs in its own terminal. Every node needs:
- `-id` - unique integer id
- `-addr` - address to listen on
- `-peers` - comma-separated `id=host:port` pairs for all other nodes
- `-data` - directory for persisted state (optional, defaults to `/tmp/raft-node-<id>`)

### 3-Node Cluster

```
# terminal 1
bin/node -id=0 -addr=:5000 -peers=1=localhost:5001,2=localhost:5002

# terminal 2
bin/node -id=1 -addr=:5001 -peers=0=localhost:5000,2=localhost:5002

# terminal 3
bin/node -id=2 -addr=:5002 -peers=0=localhost:5000,1=localhost:5001
```

### 5-Node Cluster

```
# terminal 1
bin/node -id=0 -addr=:5000 -peers=1=localhost:5001,2=localhost:5002,3=localhost:5003,4=localhost:5004

# terminal 2
bin/node -id=1 -addr=:5001 -peers=0=localhost:5000,2=localhost:5002,3=localhost:5003,4=localhost:5004

# terminal 3
bin/node -id=2 -addr=:5002 -peers=0=localhost:5000,1=localhost:5001,3=localhost:5003,4=localhost:5004

# terminal 4
bin/node -id=3 -addr=:5003 -peers=0=localhost:5000,1=localhost:5001,2=localhost:5002,4=localhost:5004

# terminal 5
bin/node -id=4 -addr=:5004 -peers=0=localhost:5000,1=localhost:5001,2=localhost:5002,3=localhost:5003
```

### Any Cluster Size

The pattern works for any odd number of nodes. For N nodes, give each node an id from 0 to N-1 and list all the others as peers.

### Things to Try

- **Watch leader election** - start all nodes and a leader is elected within a few hundred milliseconds
- **Kill the leader** - Ctrl+C a leader's terminal. A new leader is elected automatically
- **Kill a minority** - stop 1 node in a 3-node cluster or 2 nodes in a 5-node cluster. The cluster keeps working
- **Kill a majority** - stop 2 of 3 or 3 of 5 nodes. The remaining nodes keep holding elections but can never get enough votes
- **Restart a node** - start a previously killed node with the same flags. It restores its state from disk and rejoins the cluster
