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

References
[Raft extended paper](http://www.news.cs.nyu.edu/~jinyang/ds-reading/raft.pdf)
[Implementation Guide](https://thesquareplanet.com/blog/students-guide-to-raft/)
[Illustrated Raft Tutorial](http://thesecretlivesofdata.com/raft/)

