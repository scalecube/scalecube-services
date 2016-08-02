package io.scalecube.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.transform;
import static io.scalecube.transport.TransportChannel.Status.CLOSED;
import static io.scalecube.transport.TransportChannel.Status.CONNECTED;
import static io.scalecube.transport.TransportChannel.Status.CONNECT_IN_PROGRESS;

import io.scalecube.transport.utils.FutureUtils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rx.functions.Func1;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

/**
 * Represent p2p connection between two transport endpoints.
 */
final class TransportChannel {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransportChannel.class);

  private static final AttributeKey<TransportChannel> ATTR_TRANSPORT_CHANNEL = AttributeKey.valueOf("transport");

  public enum Status {
    CONNECT_IN_PROGRESS, CONNECTED, HANDSHAKE_IN_PROGRESS, HANDSHAKE_PASSED, READY, CLOSED
  }

  private final Channel channel;
  private final AtomicReference<Status> status;
  private final Func1<TransportChannel, Void> closeCallback;
  private final AtomicReference<Throwable> cause = new AtomicReference<>();
  private final SettableFuture<TransportHandshakeData> handshakeFuture = SettableFuture.create();

  private TransportChannel(Channel channel, Status initialStatus, Func1<TransportChannel, Void> closeCallback) {
    checkArgument(channel != null);
    checkArgument(initialStatus != null);
    checkArgument(closeCallback != null);
    this.channel = channel;
    this.status = new AtomicReference<>(initialStatus);
    this.closeCallback = closeCallback;
  }

  public static TransportChannel newConnectorChannel(Channel channel, Func1<TransportChannel, Void> closeCallback) {
    TransportChannel target = new TransportChannel(channel, CONNECT_IN_PROGRESS, closeCallback);
    channel.attr(TransportChannel.ATTR_TRANSPORT_CHANNEL).set(target);
    return target;
  }

  public static TransportChannel newAcceptorChannel(Channel channel, Func1<TransportChannel, Void> closeCallback) {
    TransportChannel target = new TransportChannel(channel, CONNECTED, closeCallback);
    channel.attr(TransportChannel.ATTR_TRANSPORT_CHANNEL).set(target);
    return target;
  }

  /**
   * Resolve TransportChannel by given Netty channel. It doesn't create new instance of TransportChannel.
   * 
   * @throws TransportBrokenException if given Netty channel not associated with any transport channel.
   */
  public static TransportChannel from(Channel channel) {
    TransportChannel transport = channel.attr(ATTR_TRANSPORT_CHANNEL).get();
    if (transport == null) {
      throw new TransportBrokenException("Transport not set for the given channel: " + channel);
    }
    return transport;
  }

  /**
   * Setter for {@link #handshakeFuture}. Called when handshake passed successfully (RESOLVED_OK) on both sides.
   *
   * @param handshakeData remote handshake (non null)
   */
  void setHandshakeData(TransportHandshakeData handshakeData) {
    checkArgument(handshakeData != null);
    handshakeFuture.set(handshakeData);
  }

  Channel channel() {
    return channel;
  }

  ListenableFuture<TransportHandshakeData> handshakeFuture() {
    return handshakeFuture;
  }

  /**
   * Origin/Destination of this transport.
   *
   * @return TransportEndpoint object this transport is referencing to; or {@code null} if this transport isn't READY
   *         yet
   */
  @Nullable
  public TransportEndpoint remoteEndpoint() {
    if (handshakeFuture.isDone()) {
      try {
        return handshakeFuture.get().endpoint();
      } catch (InterruptedException | ExecutionException ex) {
        LOGGER.error("Failed to get remote endpoint, ex");
      }
    }
    return null;
  }

  /**
   * Sends message to remote endpoint. Send is async operation.
   *
   * @param message message to send
   * @throws IllegalArgumentException if {@code message} is null
   */
  public void send(@CheckForNull Message message) {
    send(message, null);
  }

  /**
   * Sends message to remote endpoint. Send is async operation, if result of operation is not needed leave second
   * parameter null, otherwise pass {@link SettableFuture}. If transport channel is already closed - {@code promise}
   * will be failed with {@link TransportClosedException}.
   *
   * @param message message to send
   * @param promise promise will be completed with result of sending (void or exception)
   * @throws IllegalArgumentException if {@code message} is null
   */
  public void send(@CheckForNull Message message, @Nullable SettableFuture<Void> promise) {
    checkArgument(message != null);
    if (promise != null && getCause() != null) {
      promise.setException(getCause());
    } else {
      FutureUtils.compose(channel.writeAndFlush(message), promise);
    }
  }

  /**
   * Close transport channel, disconnect all available connections which belong to this transport channel. <br/>
   * After transport is closed it can't be opened again. New transport channel to the same endpoint can be created.<br/>
   * Close is async operation, if result of operation is not needed leave second parameter null, otherwise pass
   * {@link SettableFuture}.
   *
   * @param promise promise will be completed with result of closing (void or exception)
   */
  public void close(@Nullable SettableFuture<Void> promise) {
    close(null/* cause */, promise);
  }

  void close(Throwable cause) {
    close(cause, null/* promise */);
  }

  void close() {
    close(null/* cause */, null/* promise */);
  }

  void close(Throwable cause, SettableFuture<Void> promise) {
    this.cause.compareAndSet(null, cause != null ? cause : new TransportClosedException());
    status.set(CLOSED);
    closeCallback.call(this);
    FutureUtils.compose(channel.close(), promise);
    LOGGER.info("Closed {}", this);
  }

  /**
   * Flips the transport channel {@link #status}.
   *
   * @throws TransportBrokenException in case {@code expect} is not actual
   */
  void flip(Status expect, Status update) throws TransportBrokenException {
    if (!status.compareAndSet(expect, update)) {
      String err = "Can't set status " + update + " (expect=" + expect + ", actual=" + status + ") on channel: " + this;
      throw new TransportBrokenException(err);
    }
  }

  Throwable getCause() {
    return cause.get();
  }

  @Override
  public String toString() {
    if (getCause() == null) {
      return "TransportChannel{" + "status=" + status + ", channel=" + channel + '}';
    }
    Class clazz = getCause().getClass();
    String packageName = clazz.getPackage().getName();
    String dottedPackageName =
        Joiner.on('.').join(transform(Splitter.on('.').split(packageName), new Function<String, Character>() {
          @Override
          public Character apply(String input) {
            return input.charAt(0);
          }
        }));
    return "TransportChannel{"
        + "status=" + status
        + ", cause=[" + dottedPackageName + "." + clazz.getSimpleName() + "]"
        + ", channel=" + channel + '}';
  }

}
