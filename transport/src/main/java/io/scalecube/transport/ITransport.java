package io.scalecube.transport;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;

import java.net.InetSocketAddress;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Transport is responsible for maintaining existing p2p connections to/from other transport endpoints. Allows sending
 * messages and listen incoming messages.
 */
public interface ITransport {

  /**
   * Returns {@link TransportEndpoint} corresponding to this instance of transport.
   */
  TransportEndpoint localEndpoint();

  /**
   * Starts transport on the given endpoint so it is started to accept connection and can connect to other endpoint.
   */
  ListenableFuture<Void> start();

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
   * Stop is async operation, if result of operation is not needed leave second parameter null, otherwise pass
   * {@link SettableFuture}.
   *
   * @param promise promise will be completed with result of closing (void or exception)
   */
  void stop(@Nullable SettableFuture<Void> promise);

  /**
   * Issues connect to the given transport socketAddress. This method may be used in case if specific incarnation id of
   * remote endpoint is unknown to connect by socketAddress. So result endpoint then can be used for message sending.
   *
   * @param socketAddress socketAddress of transport endpoint to connect
   * @return Listenable future to remote transport endpoint, which is completed once the handshake is passed.
   */
  ListenableFuture<TransportEndpoint> connect(@CheckForNull InetSocketAddress socketAddress);

  /**
   * Disconnects existing transport channel to the given endpoint. If there is no connection do nothing and immediately
   * set provided promise. Close is an async operation it may cause to fail send operations either called before
   * disconnect (if their promise not set yet) or the following after disconnect since they may be assigned to existing
   * disconnecting channel instead of creating new channel.
   *
   * <p>
   * If result of operation is not needed leave second parameter null, otherwise pass {@link SettableFuture}.
   * </p>
   * 
   * @param endpoint endpoint to disconnect
   * @param promise promise will be completed with result of closing (void or exception)
   * @throws IllegalArgumentException if {@code endpoint} is null
   */
  void disconnect(@CheckForNull TransportEndpoint endpoint, @Nullable SettableFuture<Void> promise);

  /**
   * Sends message to remote endpoint. It will issue connect in case if no transport channel by given transport
   * {@code endpoint} exists already. Send is an async operation.
   *
   * @param endpoint endpoint where message will be sent
   * @param message message to send
   * @throws IllegalArgumentException if {@code message} or {@code endpoint} is null
   */
  void send(@CheckForNull TransportEndpoint endpoint, @CheckForNull Message message);

  /**
   * Sends message to remote endpoint. It will issue connect in case if no transport channel by given transport
   * {@code endpoint} exists already. Send is an async operation, if result of operation is not needed leave third
   * parameter null, otherwise pass {@link SettableFuture}. If transport channel is already closed - {@code promise}
   * will be failed with {@link TransportClosedException}.
   *
   * @param message message to send
   * @param promise promise will be completed with result of sending (void or exception)
   * @throws IllegalArgumentException if {@code message} or {@code endpoint} is null
   */
  void send(@CheckForNull TransportEndpoint endpoint, @CheckForNull Message message,
      @Nullable SettableFuture<Void> promise);

  /**
   * Returns stream of messages received from any remote endpoint regardless direction of connection. For each observers
   * subscribed to the returned observable:
   * <ul>
   * <li>{@code rx.Observer#onNext(Object)} will be invoked when some message arrived to current endpoint</li>
   * <li>{@code rx.Observer#onCompleted()} will be invoked when there is no possibility that server will receive new
   * message observable for already closed transport</li>
   * <li>{@code rx.Observer#onError(Throwable)} will not be invoked</li>
   * </ul>
   *
   * @return Observable which emit messages from remote endpoint or complete event when transport is closed
   */
  @Nonnull
  Observable<Message> listen();

}
