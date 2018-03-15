package io.scalecube.transport;

import com.google.common.base.Throwables;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Transport is responsible for maintaining existing p2p connections to/from other transports. It allows to send
 * messages to other transports and listen for incoming messages.
 */
public interface Transport {

  /**
   * Init transport with the default configuration synchronously. Starts to accept connections on local address.
   * 
   * @return transport
   */
  static Transport bindAwait() {
    return bindAwait(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the default configuration and network emulator flag synchronously. Starts to accept connections
   * on local address.
   *
   * @return transport
   */
  static Transport bindAwait(boolean useNetworkEmulator) {
    return bindAwait(TransportConfig.builder().useNetworkEmulator(useNetworkEmulator).build());
  }

  /**
   * Init transport with the given configuration synchronously. Starts to accept connections on local address.
   *
   * @return transport
   */
  static Transport bindAwait(TransportConfig config) {
    try {
      return bind(config).get();
    } catch (Exception e) {
      throw Throwables.propagate(Throwables.getRootCause(e));
    }
  }

  /**
   * Init transport with the default configuration asynchronously. Starts to accept connections on local address.
   * 
   * @return promise for bind operation
   */
  static CompletableFuture<Transport> bind() {
    return bind(TransportConfig.defaultConfig());
  }

  /**
   * Init transport with the given configuration asynchronously. Starts to accept connections on local address.
   * 
   * @param config transport config
   * @return promise for bind operation
   */
  static CompletableFuture<Transport> bind(TransportConfig config) {
    return new TransportImpl(config).bind0();
  }

  /**
   * Returns local {@link Address} on which current instance of transport listens for incoming messages.
   *
   * @return address
   */
  @Nonnull
  Address address();

  /**
   * Stop transport, disconnect all connections and release all resources which belong to this transport. After
   * transport is stopped it can't be used again. Observable returned from method {@link #listen()} will immediately
   * emit onComplete event for all subscribers.
   */
  void stop();

  /**
   * Stop transport, disconnect all connections and release all resources which belong to this transport. After
   * transport is stopped it can't be opened again. Observable returned from method {@link #listen()} will immediately
   * emit onComplete event for all subscribers. Stop is async operation, if result of operation is not needed use
   * {@link Transport#stop}, otherwise pass {@link CompletableFuture}.
   *
   * @param promise promise will be completed with result of closing (void or exception)
   */
  void stop(@CheckForNull CompletableFuture<Void> promise);


  /**
   * @return true if transport was stopped; false otherwise.
   */
  boolean isStopped();

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
   * {@link Transport#send(Address, Message)}, otherwise pass {@link CompletableFuture}.
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

  /**
   * Returns network emulator associated with this instance of transport. It always returns non null instance even if
   * network emulator is disabled by transport config. In case when network emulator is disable all calls to network
   * emulator instance will result in no operation.
   * 
   * @return network emulator
   */
  @Nonnull
  NetworkEmulator networkEmulator();

}
