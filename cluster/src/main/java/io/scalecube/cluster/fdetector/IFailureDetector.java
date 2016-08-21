package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Address;
import io.scalecube.transport.IListenable;

import rx.Observable;
import rx.Scheduler;

import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * Failure Detector component responsible for monitoring availability of other members in the cluster. This interface is
 * supposed to be used internally as part cluster membership protocol. It doesn't specify that particular node is
 * failed, but just provide information that either it is suspected or trusted at current moment of time. So it is up to
 * cluster membership or other top level component to define when suspected member is actually failed.
 *
 * @author Anton Kharenko
 */
public interface IFailureDetector extends IListenable<FailureDetectorEvent> {

  /**
   * Starts running failure detection algorithm. After started it begins to receive and send ping messages.
   */
  void start();

  /**
   * Stops running failure detection algorithm and releases occupied resources.
   */
  void stop();

  /**
   * Marks given member as SUSPECTED inside FD algorithm internals.
   */
  void suspect(Address member);

  /**
   * Marks given member as TRUSTED inside FD algorithm internals.
   */
  void trust(Address member);

  /**
   * Updates list of cluster members among which should work FD algorithm.
   */
  void setMembers(Collection<Address> members);

  /**
   * Listens for detected cluster members status changes (SUSPECTED/TRUSTED).
   */
  @Override
  Observable<FailureDetectorEvent> listen();

  /**
   * Listens for detected cluster members status changes (SUSPECTED/TRUSTED).
   */
  @Override
  Observable<FailureDetectorEvent> listen(Executor executor);

  /**
   * Listens for detected cluster members status changes (SUSPECTED/TRUSTED).
   */
  @Override
  Observable<FailureDetectorEvent> listen(Scheduler scheduler);
}
