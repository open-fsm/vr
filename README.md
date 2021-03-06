# Viewstamped Replication Library

[![Build Status](https://github.com/open-rsm/vr/workflows/CI/badge.svg)](https://github.com/open-rsm/vr/actions)
[![license](https://img.shields.io/badge/license-Apache2-orange.svg?style=flat)](https://github.com/open-rsm/vr/blob/main/LICENSE)

[Viewstamped Replication Revisited]: https://dspace.mit.edu/bitstream/handle/1721.1/71763/MIT-CSAIL-TR-2012-021.pdf?sequence=1
[Viewstamped Replication]: https://
[raft]: https://raft.github.io/raft.pdf
A library of finite state machine algorithms library (not a toolkit) is based on [Viewstamped Replication Revisited][Viewstamped Replication Revisited].

VR is a very important distributed consensus protocol. Its flexibility is better than [raft][raft], and it is much simpler than paxos in terms of engineering difficulty. It is a basic distributed algorithm developed together with paxos. This algorithm library is based on the implementation of Barbara Liskov and James Cowling to the updated version of the vr protocol. The original paper was published in 1988 and named [Viewstamped Replication: A New Primary Copy Method to Support Highly-Available Distributed Systems][Viewstamped Replication].

## Feature
This library is the minimum viable product implementation of the replication state machine and does not contain other modules, such as network transmission, log storage, etc. The advantage of such a high degree of decoupling is that users can customize the required modules according to their own needs, and this library focuses on the research of the distributed protocol itself. You can easily integrate it into your system to realize the core and key links from a single machine to a distributed system.
1. Not require any use of disk; instead it uses replicated state to provide persistence.
2. Replica pre-election strategy, after the primary node goes down, all other nodes reach a consensus in advance before the election.
3. Allows the membership of the replica group to change.
4. Node changes, with small probabilistic fluctuations.
5. The logic can be changed externally.
6. There is a clock oscillating device inside to ensure the same pace of replication nodes.

## Instructions
It is an automaton. If you try to call it, you need to give it some messages. These messages may come from the primary replicator or the backup replicator. Next, you need to wait, and the state machine will feed you some food. The organization of food is a four-tuple. This information is the core element that promotes the continuous movement of the automata.
