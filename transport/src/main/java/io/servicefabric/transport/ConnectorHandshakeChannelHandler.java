package io.servicefabric.transport;

import static io.servicefabric.transport.utils.ChannelFutureUtils.wrap;

import io.servicefabric.transport.protocol.Message;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.ScheduledFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Duplex handler. On inbound recognizes only handshake message {@link TransportData#Q_TRANSPORT_HANDSHAKE_SYNC_ACK}
 * (rest inbound messages unsupported and results in {@link TransportBrokenException}). On outbound may enqueue
 * messages. On 'channel active' starting handshake process.
 * <p/>
 * <b>NOTE:</b> this handler is not shareable (see {@link #sendMailbox}, {@link #handshakeTimer}); and should always run
 * in different executor than io-thread.
 */
final class ConnectorHandshakeChannelHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(ConnectorHandshakeChannelHandler.class);

  final ITransportSpi transportSpi;
  Queue<WriteAndFlush> sendMailbox;
  ScheduledFuture handshakeTimer;

  static class WriteAndFlush {
    final Object msg;
    final ChannelPromise promise;

    WriteAndFlush(Object msg, ChannelPromise promise) {
      this.msg = msg;
      this.promise = promise;
    }
  }

  ConnectorHandshakeChannelHandler(ITransportSpi transportSpi) {
    this.transportSpi = transportSpi;
    this.sendMailbox = new ArrayBlockingQueue<>(transportSpi.getSendHwm(), true/* fair */);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
    transport.flip(TransportChannel.Status.CONNECT_IN_PROGRESS, TransportChannel.Status.CONNECTED);

    final TransportData handshake = TransportData.newData(transport.transportSpi.getLocalMetadata()).build();
    int handshakeTimeout = transport.transportSpi.getHandshakeTimeout();
    handshakeTimer = transport.channel.eventLoop().schedule(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("HANDSHAKE_SYNC({}) timeout, connector: {}", handshake, transport);
        transport.close(new TransportHandshakeException(transport, handshake, new TimeoutException()));
      }
    }, handshakeTimeout, TimeUnit.MILLISECONDS);

    transport.flip(TransportChannel.Status.CONNECTED, TransportChannel.Status.HANDSHAKE_IN_PROGRESS);

    ChannelFuture channelFuture =
        ctx.writeAndFlush(new Message(handshake, TransportHeaders.QUALIFIER, TransportData.Q_TRANSPORT_HANDSHAKE_SYNC));
    channelFuture.addListener(wrap(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          LOGGER.debug("HANDSHAKE_SYNC({}) not sent, connector: {}", handshake, transport);
          cancelHandshakeTimer();
          transport.close(new TransportHandshakeException(transport, handshake, future.cause()));
        }
      }
    }));

    super.channelActive(ctx);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    boolean offer = sendMailbox.offer(new WriteAndFlush(msg, promise));
    if (!offer && promise != null) {
      TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
      String message = "sendMailbox full: capacity=" + transportSpi.getSendHwm() + ", size=" + sendMailbox.size();
      promise.setFailure(new TransportMessageException(transport, message, msg));
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    Message message = (Message) msg;
    if (!TransportData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK.equals(message.header(TransportHeaders.QUALIFIER))) {
      throw new TransportBrokenException("Received unsupported " + msg
          + " (though expecting only Q_TRANSPORT_HANDSHAKE_SYNC_ACK)");
    }

    TransportData handshake = (TransportData) message.data();
    final TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
    if (handshake.isResolvedOk()) {
      cancelHandshakeTimer();
      transport.setRemoteHandshake(handshake);
      transport.transportSpi.resetDueHandshake(transport.channel);
      transport.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_PASSED);
      LOGGER.debug("HANDSHAKE passed on connector: {}", transport);
      writeAndFlushSendMailbox(ctx);
      transport.flip(TransportChannel.Status.HANDSHAKE_PASSED, TransportChannel.Status.READY);
      LOGGER.debug("Set READY on connector: {}", transport);
    } else {
      LOGGER.debug("HANDSHAKE({}) not passed, connector: {}", handshake, transport);
      cancelHandshakeTimer();
      transport.close(new TransportHandshakeException(transport, handshake));
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    cancelHandshakeTimer();
    cleanupSendMailbox(ctx);
    super.channelInactive(ctx);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
    cancelHandshakeTimer();
    cleanupSendMailbox(ctx);
    super.close(ctx, future);
  }

  private void writeAndFlushSendMailbox(ChannelHandlerContext ctx) {
    while (!sendMailbox.isEmpty()) {
      WriteAndFlush waf = sendMailbox.poll();
      ctx.writeAndFlush(waf.msg, waf.promise);
    }
  }

  private void cleanupSendMailbox(ChannelHandlerContext ctx) {
    TransportChannel transport = transportSpi.getTransportChannel(ctx.channel());
    Throwable transportCause = transport.getCause();
    Throwable cause = transportCause != null ? transportCause : new TransportClosedException(transport);
    while (!sendMailbox.isEmpty()) {
      WriteAndFlush waf = sendMailbox.poll();
      if (waf.promise != null) {
        waf.promise.setFailure(cause);
      }
    }
  }

  private void cancelHandshakeTimer() {
    if (handshakeTimer != null) {
      handshakeTimer.cancel(true);
    }
  }
}
