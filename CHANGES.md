# Changes

## 1.0.8 / 2018-02-07

* Update ServiceRegistry api, expose 'methods' on ServiceInstance

## 1.0.7 / 2017-12-05

* Introduce ServiceCall.invokeAll service endpoints 
* Metrics added to ServiceCall request / response / error rates KPIs 
* Add member host/port overrides on member config
 
 Shout-out:
- Mike Barker on first contribution.
  
  
## 1.0.6 / 2017-09-20

* Supports graceful shutdown of cluster member by spreading leaving notification
* Support rx.Observables as service response type
* Update dependencies introduce performance improvements 

## 1.0.5 / 2017-08-05

* Add @Inject annotation for services without special params
* Improve ClusterConfig API

## 1.0.4 / 2017-06-05

* Fixed proxy api resolve issue when it call inside the service
* Use separate transport for services and use cluster for discovery only
* Fix issue with binding transport to IPv6 address
* Add @ServiceProxy inject annotation 
* Reduce tech debt and improve testing

## 1.0.3 / 2016-12-28

* Improve ClusterConfig API
* Fix cluster backpressure issue and gossip storm issue on initial sync
* Add possibility to change Cluster metadata dynamically after cluster started
* Move cluster join methods to Cluster interface and make ClusterImpl package private
* Reduce GC-load on message send via using Netty's voidPromise
* Transport API improvements
* Stabilize membership protocol
* Improve reliability of GossipProtocol under adverse conditions (message loss 50%)
* Support service dispatchers
* Do not create observable filter on each call to cluster's listen method
* Fix issue when starting many nodes at once throw exception address already in use 

## 1.0.2 / 2016-12-05

* Support service tags

## 1.0.1 / 2016-11-22

* Fix backpressure issue

## 1.0.0 / 2016-11-13

* Initial stable release
