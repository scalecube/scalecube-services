package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
final class NetworkEmulatorHandler extends ChannelOutboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulatorHandler.class);

  private final Map<TransportEndpoint, NetworkEmulatorSettings> networkSettings = new ConcurrentHashMap<>();

  private NetworkEmulatorSettings defaultSettings = new NetworkEmulatorSettings(0, 0);

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
    NetworkEmulatorSettings networkSettings = resolveNetworkSettings(ctx.channel());

    // Emulate message loss
    boolean isLost = networkSettings.evaluateLost();
    if (isLost) {
      if (promise != null) {
        promise.setFailure(new RuntimeException("NETWORK_BREAK detected, not sent " + msg));
      }
      return;
    }

    // Emulate message delay
    int delay = (int) networkSettings.evaluateDelay();
    if (delay > 0) {
      try {
        ctx.channel().eventLoop().schedule(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            NetworkEmulatorHandler.super.write(ctx, msg, promise);
            return null;
          }
        }, delay, TimeUnit.MILLISECONDS);
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

  private NetworkEmulatorSettings resolveNetworkSettings(Channel channel) {
    InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
    NetworkEmulatorSettings networkSettings = defaultSettings;
    for (Map.Entry<TransportEndpoint, NetworkEmulatorSettings> settings : this.networkSettings.entrySet()) {
      TransportEndpoint key = settings.getKey();
      if (remoteAddress.getHostName().equals(key.host()) && remoteAddress.getPort() == key.port()) {
        networkSettings = settings.getValue();
      }
    }
    return networkSettings;
  }

  public void setNetworkSettings(TransportEndpoint destination, int lostPercent, int mean) {
    NetworkEmulatorSettings settings = new NetworkEmulatorSettings(lostPercent, mean);
    networkSettings.put(destination, settings);
    LOGGER.debug("Set {} for messages to: {}", settings, destination);
  }

  public void setDefaultNetworkSettings(int lostPercent, int delay) {
    defaultSettings = new NetworkEmulatorSettings(lostPercent, delay);
    LOGGER.debug("Set default {}", defaultSettings);
  }

  public void blockMessagesTo(TransportEndpoint destination) {
    networkSettings.put(destination, new NetworkEmulatorSettings(100, 0));
    LOGGER.debug("Block messages to: {}", destination);
  }

  public void unblockAll() {
    networkSettings.clear();
    LOGGER.debug("Unblock all messages");
  }
}
