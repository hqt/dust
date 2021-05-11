# Dust
**Dust** stands for Distributed Database in Rust

*Dust* is a lightweight, distributed relational database, which uses [SQLite](https://www.sqlite.org/) as its storage engine. Forming a cluster is very straightforward, it gracefully handles leader elections, and tolerates failures of machines, including the leader. Dust is available for Linux, macOS, and Microsoft Windows.

Dust is based on the idea of [rqlite](https://github.com/rqlite/rqlite) and its implementation.

### Why?
Dust gives you the functionality of a [rock solid](http://www.sqlite.org/testing.html), fault-tolerant, replicated relational database, but with very **easy installation, deployment, and operation**. With it you've got a **lightweight** and **reliable distributed relational data store**. Think [etcd](https://github.com/coreos/etcd/) or [Consul](https://github.com/hashicorp/consul), but with relational data modelling also available.

You could use Dust as part of a larger system, as a central store for some critical relational data, without having to run larger, more complex distributed databases.

Finally, if you're interested in understanding how distributed systems actually work, **Dust is a good example to study**. Much thought has gone into its [design](https://github.com/hqt/dust/blob/master/DOC/DESIGN.md) and implementation, with clear separation between the various components, including storage, distributed consensus, and API.

### How?
Dust uses [Raft](https://raft.github.io/) to achieve consensus across all the instances of the SQLite databases, ensuring that every change made to the system is made to a quorum of SQLite databases, or none at all. You can learn more about the design [here](https://github.com/hqt/dust/blob/master/DOC/DESIGN.md).

### Key features
- Trivially easy to deploy, with no need to separately install SQLite.
- Fully replicated production-grade SQL database.
- [Production-grade](https://github.com/tikv/raft-rs) distributed consensus system.
- A form of transaction support.

## Performance
Dust replicates SQLite for fault-tolerance. It does not replicate it for performance. In fact performance is reduced somewhat due to the network round-trips.

Depending on your machine (particularly its IO performance) and network, individual INSERT performance could be anything from 10 operations per second to more than 200 operations per second. 

## Limitations
* Only SQL statements that are [__deterministic__](https://www.sqlite.org/deterministic.html) are safe to use with rqlite, because statements are committed to the Raft log before they are sent to each node. In other words, rqlite performs _statement-based replication_. For example, the following statement could result in a different SQLite database under each node:
```
INSERT INTO foo (n) VALUES(random());
```
* Technically this is not supported, but you can directly read the SQLite under any node at anytime, assuming you run in "on-disk" mode. However there is no guarantee that the SQLite file reflects all the changes that have taken place on the cluster unless you are sure the host node itself has received and applied all changes.
* In case it isn't obvious, Dust does not replicate any changes made directly to any underlying SQLite file, when run in "on disk" mode. **If you change the SQLite file directly, you will cause rqlite to fail**. Only modify the database via the HTTP API.
* SQLite dot-commands such as `.schema` or `.tables` are not directly supported by the API, but the rqlite CLI supports some very similar functionality. This is because those commands are features of the `sqlite3` command, not SQLite itself.
