

# Changes

## 1.0.6-SNAPSHOT

* Support cluster cluster.leave() and spread gossip notification that node is dead.
* Support cluster graceful shutdown waiting for cluster to leave() and only than shutdown().
* Support rx.Observables are service response type.
* Update dependencies introduce performance improvements: 
  * request reply 85K messages per second.
  * message stream 166K messages per second. 
