# Design

# Dust 


## Node design
The diagram below shows a high-level view of an Dust node.

                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐     ┌ ─ ─ ─ ─ ┐
                             Clients                    Other
                 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘     │  Nodes  │
                                │                     ─ ─ ─ ─ ─
                                │                        ▲
                                │                        │
                                │                        │
                                ▼                        ▼
                 ┌─────────────────────────────┐ ┌────────────────┐
                 │           HTTP(S)           │ │     gRPC       │  
                 └─────────────────────────────┘ └────────────────┘
                 ┌────────────────────────────────────────────────┐
                 │     Raft consensus module (tikv/raft-rs)       │
                 └────────────────────────────────────────────────┘
                 ┌────────────────────────────────────────────────┐
                 │               rusqlite/rusqlite                │
                 └────────────────────────────────────────────────┘
                 ┌────────────────────────────────────────────────┐
                 │                   sqlite3.c                    │
                 └────────────────────────────────────────────────┘
                 ┌────────────────────────────────────────────────┐
                 │                 RAM or disk                    │
                 └────────────────────────────────────────────────┘

## Raft
A complete Raft model contains 4 essential parts:
1. Consensus Module, the core consensus algorithm module;
2. Log, the place to keep the Raft logs;
3. State Machine, the place to save the user data;
4. Transport, the network layer for communication.

[tikv/raft-rs](https://github.com/tikv/raft-rs) implementation includes the core Consensus Module only, not the other parts. This design is different with [hashicorp/raft](https://github.com/hashicorp/raft), which is more an end-to-end solution for a Raft implementation. As the result, Dust must make some opinionate choices:
- Log: Implement own storage, which is inspired by [hyperledger/sawtooth-raft](https://github.com/hyperledger/sawtooth-raft)
- State Machine: own implementation.
- Transport: Using [gRPC](https://github.com/hyperium/tonic) for performance and extensibility.

The design docs for those parts are TBD.

## File system
### Raft
The Raft layer always creates a file -- it creates the _Raft log_. The log stores the set of committed SQLite commands, in the order which they were executed. This log is authoritative record of every change that has happened to the system. It may also contain some read-only queries as entries, depending on read-consistency choices.

### SQLite
By default the SQLite layer doesn't create a file. Instead it creates the database in RAM. Dust can create the SQLite database on disk, if so configured at start-time.

## Log Compaction and Truncation
Dust automatically performs log compaction, so that disk usage due to the log remains bounded. After a configurable number of changes Dust snapshots the SQLite database, and truncates the Raft log. This is a technical feature of the Raft consensus system, and most users of Dust need not be concerned with this.
