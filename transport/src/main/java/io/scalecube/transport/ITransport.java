package io.scalecube.transport;

import rx.Observable;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * Transport is responsible for maintaining existing p2p connections to/from other transports. It allows sending
 * messages and listen incoming messages.
 */
public interface ITransport {

  /**
   * Returns local {@link Address} on which current instance of transport listens for incoming messages.
   */
  Address address();

  /**
   * Stop transport, disconnect all available connections which belong to this transport. <br/>
   * After transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will
   * immediately emit onComplete event for all subscribers.
   */
  void stop();

  /**
   * Stop transport, disconnect all available connections which belong to this transport. <br/>
   * After transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will
   * immediately emit onComplete event for all subscribers. <br/>
   * Stop is async operation, if result of operation is not needed use {@link ITransport#stop}, otherwise pass
   * {@link CompletableFuture}.
   *
   * @param promise promise will be completed with result of closing (void or exception)
   */
  void stop(@CheckForNull CompletableFuture<Void> promise);

  /**
   * Sends message to the given address. It will issue connect in case if no transport channel by given transport
   * {@code address} exists already. Send is an async operation.
   *
   * @param address address where message will be sent
   * @param message message to send
   * @throws IllegalArgumentException if {@code message} or {@code address} is null
   */
  void send(@CheckForNull Address address, @CheckForNull Message message);

  /**
   * Sends message to the given address. It will issue connect in case if no transport channel by given {@code address}
   * exists already. Send is an async operation, if result of operation is not needed use
   * {@link ITransport#send(Address, Message)}, otherwise pass {@link CompletableFuture}.
   *
   * @param message message to send
   * @param promise promise will be completed with result of sending (void or exception)
   * @throws IllegalArgumentException if {@code message} or {@code address} is null
   */
  void send(@CheckForNull Address address, @CheckForNull Message message,
      @CheckForNull CompletableFuture<Void> promise);

  /**
   * Returns stream of received messages. For each observers subscribed to the returned observable:
   * <ul>
   * <li>{@code rx.Observer#onNext(Object)} will be invoked when some message arrived to current transport</li>
   * <li>{@code rx.Observer#onCompleted()} will be invoked when there is no possibility that server will receive new
   * message observable for already closed transport</li>
   * <li>{@code rx.Observer#onError(Throwable)} will not be invoked</li>
   * </ul>
   *
   * @return Observable which emit received messages or complete event when transport is closed
   */
  @Nonnull
  Observable<Message> listen();

}
