package io.servicefabric.cluster.fdetector;

import io.servicefabric.cluster.ClusterEndpoint;

import rx.Observable;

import java.util.Collection;

/**
 * Failure Detector component responsible for monitoring availability of other members in the cluster. This interface is
 * supposed to be used internally as part cluster membership protocol. It doesn't specify that particular node is
 * failed, but just provide information that either it is suspected or trusted at current moment of time. So it is up to
 * cluster membership or other top level component to define when suspected member is actually failed.
 *
 * @author Anton Kharenko
 */
public interface IFailureDetector {

  /** Starts running failure detection algorithm. After started it begins to receive and send failure detection messages */
  void start();

  /** Stops running failure detection algorithm and releases occupied resources. */
  void stop();

  /** Listens for detected cluster members status changes (SUSPECTED/TRUSTED) by failure detector. */
  Observable<FailureDetectorEvent> listenStatus();

  /** Marks given member as SUSPECTED inside FD algorithm internals. */
  void suspect(ClusterEndpoint member);

  /** Marks given member as TRUSTED inside FD algorithm internals. */
  void trust(ClusterEndpoint member);

  /** Updates list of cluster members among which should work FD algorithm. */
  void setClusterMembers(Collection<ClusterEndpoint> members);
}
