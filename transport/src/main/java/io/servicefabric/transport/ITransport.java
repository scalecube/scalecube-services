package io.servicefabric.transport;

import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transport is responsible for maintaining existing {@link ITransportChannel} to/from other transport endpoints.
 */
public interface ITransport {

  /**
   * Starts transport on the given endpoint so it is started to accept connection and can connect to other endpoint.
   */
  void start();

  /**
   * Returns (or issues connect and returns) transport channel by given transport {@code endpoint}.
   * 
   * @param endpoint by which to get or connect transport channel
   * @throws IllegalArgumentException if {@code endpoint} is null
   * @return non null transport object
   */
  @Nonnull
  ITransportChannel to(@CheckForNull TransportEndpoint endpoint);

  /**
   * Returns stream of messages received from any remote endpoint regardless direction of connection. For each observers subscribed to the
   * returned observable:
   * <ul>
   * <li>{@link rx.Observer#onNext(Object)} will be invoked when some message arrived to current endpoint</li>
   * <li>{@link rx.Observer#onCompleted()} will be invoked when there is no possibility that server will receive new message observable for
   * already closed transport</li>
   * <li>{@link rx.Observer#onError(Throwable)} will not be invoked</li>
   * </ul>
   * 
   * @return Observable which emit messages from remote endpoint or complete event when transport is closed
   */
  @Nonnull
  Observable<TransportMessage> listen();

  /**
   * Stop transport, disconnect all available connections which belong to this transport. <br/>
   * After transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will immediately emit onComplete
   * event for all subscribers.
   */
  void stop();

  /**
   * Stop transport, disconnect all available connections which belong to this transport. <br/>
   * After transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will immediately emit onComplete
   * event for all subscribers. <br/>
   * Stop is async operation, if result of operation is not needed leave second parameter null, otherwise pass {@link SettableFuture}.
   * 
   * @param promise promise will be completed with result of closing (void or exception)
   */
  void stop(@Nullable SettableFuture<Void> promise);
}
