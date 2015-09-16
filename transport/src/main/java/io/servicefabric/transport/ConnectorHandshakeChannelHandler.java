package io.servicefabric.transport;

import static io.servicefabric.transport.utils.ChannelFutureUtils.wrap;

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
 * Duplex handler. On inbound recognizes only handshake message
 * {@link TransportHandshakeData#Q_TRANSPORT_HANDSHAKE_SYNC_ACK} (rest inbound messages unsupported and results in
 * {@link TransportBrokenException}). On outbound may enqueue messages. On 'channel active' starting handshake process.
 * <p/>
 * <b>NOTE:</b> this handler is not shareable (see {@link #sendMailbox}, {@link #handshakeTimeout}); and should always
 * run in different executor than io-thread.
 */
final class ConnectorHandshakeChannelHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(ConnectorHandshakeChannelHandler.class);

  private final ITransportSpi transportSpi;
  private final Queue<WriteAndFlush> sendMailbox;
  private ScheduledFuture handshakeTimeout;

  ConnectorHandshakeChannelHandler(ITransportSpi transportSpi) {
    this.transportSpi = transportSpi;
    this.sendMailbox = new ArrayBlockingQueue<>(transportSpi.getSendHighWaterMark(), true/* fair */);
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    final TransportChannel transportChannel = TransportChannel.from(ctx.channel());
    transportChannel.flip(TransportChannel.Status.CONNECT_IN_PROGRESS, TransportChannel.Status.CONNECTED);

    final TransportHandshakeData handshake = TransportHandshakeData.create(transportSpi.localEndpoint());
    int handshakeTimeout = transportSpi.getHandshakeTimeout();
    this.handshakeTimeout = transportChannel.channel().eventLoop().schedule(new Runnable() {
      @Override
      public void run() {
        LOGGER.debug("HANDSHAKE_SYNC({}) timeout, connector: {}", handshake, transportChannel);
        transportChannel.close(new TransportHandshakeException("Handshake timeout on " + transportChannel,
            new TimeoutException()));
      }
    }, handshakeTimeout, TimeUnit.MILLISECONDS);

    transportChannel.flip(TransportChannel.Status.CONNECTED, TransportChannel.Status.HANDSHAKE_IN_PROGRESS);

    ChannelFuture channelFuture =
        ctx.writeAndFlush(new Message(handshake, TransportHeaders.QUALIFIER,
            TransportHandshakeData.Q_TRANSPORT_HANDSHAKE_SYNC));
    channelFuture.addListener(wrap(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
          LOGGER.debug("HANDSHAKE_SYNC({}) not sent, connector: {}", handshake, transportChannel);
          cancelHandshakeTimeout();
          transportChannel.close(new TransportHandshakeException("Failed to send handshake to " + transportChannel,
              future.cause()));
        }
      }
    }));

    super.channelActive(ctx);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
    TransportChannel transport = TransportChannel.from(ctx.channel());

    // Check if transport wasn't closed already
    if (transport.getCause() != null) {
      promise.setFailure(transport.getCause());
      return;
    }

    // Put to mailbox
    boolean offeredSuccessfully = sendMailbox.offer(new WriteAndFlush(msg, promise));

    // Check mailbox capacity wasn't exceeded
    if (!offeredSuccessfully && promise != null) {
      String message =
          "Failed to send message " + msg + ". Mailbox is full (capacity=" + transportSpi.getSendHighWaterMark()
              + ", size=" + sendMailbox.size() + ")";
      promise.setFailure(new TransportMessageException(message));
    }
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    Message message = (Message) msg;
    if (!TransportHandshakeData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK.equals(message.header(TransportHeaders.QUALIFIER))) {
      throw new TransportBrokenException("Received unsupported " + msg
          + " (though expecting only Q_TRANSPORT_HANDSHAKE_SYNC_ACK)");
    }

    TransportHandshakeData handshakeResponse = message.data();
    final TransportChannel transportChannel = TransportChannel.from(ctx.channel());
    if (handshakeResponse.isResolvedOk()) {
      cancelHandshakeTimeout();
      transportChannel.setHandshakeData(handshakeResponse);
      transportSpi.resetDueHandshake(transportChannel.channel());
      transportChannel.flip(TransportChannel.Status.HANDSHAKE_IN_PROGRESS, TransportChannel.Status.HANDSHAKE_PASSED);
      LOGGER.info("HANDSHAKE passed on connector: {}", transportChannel);
      writeAndFlushSendMailbox(ctx);
      transportChannel.flip(TransportChannel.Status.HANDSHAKE_PASSED, TransportChannel.Status.READY);
      LOGGER.info("Set READY on connector: {}", transportChannel);
    } else {
      LOGGER.info("HANDSHAKE({}) not passed, connector: {}", handshakeResponse, transportChannel);
      cancelHandshakeTimeout();
      transportChannel.close(new TransportHandshakeException(handshakeResponse.explain()));
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    cancelHandshakeTimeout();
    cleanupSendMailbox(ctx);
    super.channelInactive(ctx);
  }

  @Override
  public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
    cancelHandshakeTimeout();
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
    TransportChannel transport = TransportChannel.from(ctx.channel());
    Throwable transportCause = transport.getCause();
    Throwable cause = transportCause != null ? transportCause : new TransportClosedException();
    while (!sendMailbox.isEmpty()) {
      WriteAndFlush waf = sendMailbox.poll();
      if (waf.promise != null) {
        waf.promise.setFailure(cause);
      }
    }
  }

  private void cancelHandshakeTimeout() {
    if (handshakeTimeout != null) {
      handshakeTimeout.cancel(true);
    }
  }

  private static class WriteAndFlush {
    final Object msg;
    final ChannelPromise promise;

    WriteAndFlush(Object msg, ChannelPromise promise) {
      this.msg = msg;
      this.promise = promise;
    }
  }
}
