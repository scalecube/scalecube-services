package io.scalecube.cluster.fdetector;

import rx.Observable;

/**
 * Failure Detector component responsible for monitoring availability of other members in the cluster. This interface is
 * supposed to be used internally as part cluster membership protocol. It doesn't specify that particular node is
 * failed, but just provide information that either it is suspected or trusted at current moment of time. So it is up to
 * cluster membership or other top level component to define when suspected member is actually failed.
 *
 * @author Anton Kharenko
 */
public interface FailureDetector {

  /**
   * Starts running failure detection algorithm. After started it begins to receive and send ping messages.
   */
  void start();

  /**
   * Stops running failure detection algorithm and releases occupied resources.
   */
  void stop();

  /**
   * Listens for results of ping checks (ALIVE/SUSPECT) done periodically by failure detector.
   */
  Observable<FailureDetectorEvent> listen();

}
