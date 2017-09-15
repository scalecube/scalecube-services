

# Changes

## 1.0.6

* Support cluster cluster.leave() spread gossip notification that node is dead.
* Support cluster graceful shutdown waiting for cluster to cluster.leave() and only than cluster.shutdown().
* Support rx.Observables are service response type.
* Update dependencies introduce performance improvements: 
  * request reply 85K messages per second.
  * message stream 166K messages per second. 
