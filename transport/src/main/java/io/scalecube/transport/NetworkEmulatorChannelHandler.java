package io.scalecube.transport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
final class NetworkEmulatorChannelHandler extends ChannelOutboundHandlerAdapter {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulatorChannelHandler.class);

  private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings;

  NetworkEmulatorChannelHandler(Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings) {
    this.networkSettings = networkSettings;
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
    Message message = (Message) msg;
    String qualifier = message.header(TransportHeaders.QUALIFIER);
    if (TransportHandshakeData.Q_TRANSPORT_HANDSHAKE_SYNC.equals(qualifier)
        || TransportHandshakeData.Q_TRANSPORT_HANDSHAKE_SYNC_ACK.equals(qualifier)) {
      super.write(ctx, msg, promise);
      return;
    }

    // Resolve network settings
    InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    NetworkEmulatorSettings networkSettings = NetworkEmulatorSettings.defaultSettings();
    for (Map.Entry<TransportEndpoint, NetworkEmulatorSettings> settings : this.networkSettings.entrySet()) {
      TransportEndpoint key = settings.getKey();
      if (remoteAddress.getHostName().equals(key.host()) && remoteAddress.getPort() == key.port()) {
        networkSettings = settings.getValue();
      }
    }

    // Apply network settings
    if (networkSettings.evaluateLost()) {
      if (promise != null) {
        promise.setFailure(new RuntimeException("NETWORK_BREAK detected, not sent " + msg));
      }
      return;
    }
    int timeToSleep = (int) networkSettings.evaluateDelay();
    if (timeToSleep > 0) {
      try {
        ctx.channel().eventLoop().schedule(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            NetworkEmulatorChannelHandler.super.write(ctx, msg, promise);
            return null;
          }
        }, timeToSleep, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        if (promise != null) {
          String warn = "Rejected " + msg + " on " + ctx.channel();
          LOGGER.warn(warn);
          promise.setFailure(new RuntimeException(warn, e));
        }
      }
      return;
    }
    super.write(ctx, msg, promise);
  }
}
