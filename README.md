# Distributed-Key-Value-Store

## Contents
This repo contains an implementation of RAFT written in golang.
It also contains an implementation of a distributed key value store built on top of RAFT.

## RAFT
Raft is implemented as a Go object type with associated methods, meant to be used as a pluggable module in a larger service. A set of Raft instances talk to each other with RPCs to maintain replicated logs. The Raft interface supports an indefinite sequence of log entries numbered with index numbers. The log entry with a given index will eventually be committed. Raft sends the log entries to the larger service for it to execute.

Implementations

- Leader election and heartbeats. A single leader to be elected, for the leader to remain the leader if there are no failures, and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost.

- Raft keeps a consistent, replicated log of operations. A call to Start() at the leader starts the process of adding a new operation to the log; the leader sends the new operation to the other servers in AppendEntries RPCs.

- Raft persists the necessary parts of its state by using a Persister object. Raft initializes its state from that Persister, and uses it to save its persistent state each time the state changes. 

- Raft cooperates with the service to save space by Log Compaction: From time to time the service will persistently store a "snapshot" of its current state, and Raft will discard log entries that precede the snapshot. When a server restarts (or falls far behind the leader and must catch up), the server first installs a snapshot and then replays log entries from after the point at which the snapshot was created.

References
- [Raft extended paper](http://www.news.cs.nyu.edu/~jinyang/ds-reading/raft.pdf)
- [Implementation Guide](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [Illustrated Raft Tutorial](http://thesecretlivesofdata.com/raft/)

## Fault Tolerant, Sharded Key-Value Store

### API
The service maintains a sharded database of key/value pairs & supports three RPCs
- Put(key, value): replaces the value for a particular key in the database
- Append(key, arg): appends arg to key's value. An Append to a non-existant key acts like a Put
- Get(key): fetches the current value for a key

The service is a replicated state machine consisting of several kvservers. The client code should try different kvservers it knows about until one responds positively. As long as a client can contact a kvraft server that is a Raft leader in a majority partition, its operations should eventually succeed.

The service ensures that Get(), Put(), and Append return results that are linearizable. That is, completed application calls to the Get(), Put(), and Append() methods must appear to all clients to have affected the service in the same linear order, even in there are failures and leader changes. A Get(key) that starts after a completed Put(key, …) or Append(key, …) sees the value written by the most recent Put(key, …) or Append(key, …) in the linear order. Completed calls also have exactly-once semantics.

### Sharding Details
The key/value storage system "shards," or partitions, the keys over a set of replica groups. A shard is a subset of the key/value pairs; for example, all the keys starting with "a" might be one shard, all the keys starting with "b" another, etc. The reason for sharding is performance. Each replica group handles puts and gets for just a few of the shards, and the groups operate in parallel; thus total system throughput (puts and gets per unit time) increases in proportion to the number of groups.

The system has two main components. First, a set of replica groups. Each replica group is responsible for a subset of the shards. A replica consists of a handful of servers that use Raft to replicate the group's shards. The second component is the "shard master". The shard master decides which replica group should serve each shard; this information is called the configuration. The configuration changes over time. Clients consult the shard master in order to find the replica group for a key, and replica groups consult the master in order to find out what shards to serve. There is a single shard master for the whole system, implemented as a fault-tolerant service using Raft.

A sharded storage system is also be able to shift shards among replica groups. One reason is that some groups may become more loaded than others, so that shards need to be moved to balance the load. Another reason is that replica groups may join and leave the system: new replica groups may be added to increase capacity, or existing replica groups may be taken offline for repair or retirement.

In order to handle shard reconfiguration -- changes in the assignment of shards to groups these invariants hold. Within a single replica group, all group members must agree on when a reconfiguration occurs relative to client Put/Append/Get requests. For example, a Put may arrive at about the same time as a reconfiguration that causes the replica group to stop being responsible for the shard holding the Put's key. All replicas in the group must agree on whether the Put occurred before or after the reconfiguration. If before, the Put should take effect and the new owner of the shard will see its effect; if after, the Put won't take effect and client must re-try at the new owner. 

In order to ensure this each replica group uses Raft to log not just the sequence of Puts, Appends, and Gets but also the sequence of reconfigurations. Also the system ensures that at most one replica group is serving requests for each shard at any one time.

Reconfiguration also requires interaction among the replica groups to move the contents of shards across groups.

This system's general architecture (a configuration service and a set of replica groups) follows the same general pattern as Flat Datacenter Storage, BigTable, Spanner, FAWN, Apache HBase, Rosebud, Spinnaker, and many others.

The shardmaster manages a sequence of numbered configurations. Each configuration describes a set of replica groups and an assignment of shards to replica groups. Whenever this assignment needs to change, the shard master creates a new configuration with the new assignment. Key/value clients and servers contact the shardmaster when they want to know the current (or a past) configuration.


### Garbage collection of state
When a replica group loses ownership of a shard, that replica group eliminates the keys that it lost from its database. 

### Client requests during configuration changes
The simplest way to handle configuration changes is to disallow all client operations until the transition has completed. While conceptually simple, this approach is not feasible in production-level systems; it results in long pauses for all clients whenever machines are brought in or taken out. A better solution would be if the system continued serving shards that are not affected by the ongoing configuration change.

This system ensures that if some shard S is not affected by a configuration change from C to C', client operations to S should continue to succeed while a replica group is still in the process of transitioning to C'.

Also, replica groups start serving shards the moment they are able to, even if a configuration is still ongoing. 