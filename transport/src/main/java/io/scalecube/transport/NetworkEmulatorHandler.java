package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@ChannelHandler.Sharable
final class NetworkEmulatorHandler extends ChannelOutboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulatorHandler.class);

  private final Map<Address, NetworkEmulatorSettings> networkSettings = new ConcurrentHashMap<>();

  private final AtomicLong totalMessageSentCount = new AtomicLong();

  private final AtomicLong totalMessageLostCount = new AtomicLong();

  private volatile NetworkEmulatorSettings defaultSettings = new NetworkEmulatorSettings(0, 0);

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
    NetworkEmulatorSettings networkSettings = resolveNetworkSettings(ctx.channel());
    totalMessageSentCount.incrementAndGet();

    // Emulate message loss
    boolean isLost = networkSettings.evaluateLost();
    if (isLost) {
      totalMessageLostCount.incrementAndGet();
      if (promise != null) {
        promise.setFailure(new NetworkEmulatorException("NETWORK_BREAK detected, not sent " + msg));
      }
      return;
    }

    // Emulate message delay
    int delay = (int) networkSettings.evaluateDelay();
    if (delay > 0) {
      try {
        ctx.channel().eventLoop().schedule((Callable<Void>) () -> {
          ctx.writeAndFlush(msg, promise);
          return null;
        }, delay, TimeUnit.MILLISECONDS);
      } catch (RejectedExecutionException e) {
        String warn = "Rejected " + msg + " on " + ctx.channel();
        LOGGER.warn(warn, e);
        if (promise != null) {
          promise.setFailure(new NetworkEmulatorException(warn));
        }
      }
      return;
    }

    super.write(ctx, msg, promise);
  }

  private NetworkEmulatorSettings resolveNetworkSettings(Channel channel) {
    InetSocketAddress remoteSocketAddress = (InetSocketAddress) channel.remoteAddress();
    Address remoteAddress = Address.create(remoteSocketAddress.getHostName(), remoteSocketAddress.getPort());
    return networkSettings.containsKey(remoteAddress) ? networkSettings.get(remoteAddress) : defaultSettings;
  }

  public void setNetworkSettings(Address destination, int lossPercent, int mean) {
    NetworkEmulatorSettings settings = new NetworkEmulatorSettings(lossPercent, mean);
    networkSettings.put(destination, settings);
    LOGGER.debug("Set {} for messages to: {}", settings, destination);
  }

  public void setDefaultNetworkSettings(int lossPercent, int delay) {
    defaultSettings = new NetworkEmulatorSettings(lossPercent, delay);
    LOGGER.debug("Set default {}", defaultSettings);
  }

  public void block(Address destination) {
    networkSettings.put(destination, new NetworkEmulatorSettings(100, 0));
    LOGGER.debug("Block messages to: {}", destination);
  }

  public void block(Collection<Address> destinations) {
    for (Address destination : destinations) {
      networkSettings.put(destination, new NetworkEmulatorSettings(100, 0));
    }
    LOGGER.debug("Block messages to: {}", destinations);
  }

  public void unblock(Address destination) {
    networkSettings.remove(destination);
    LOGGER.debug("Unblock messages to: {}", destination);
  }

  public void unblockAll() {
    networkSettings.clear();
    LOGGER.debug("Unblock all messages");
  }

  public long totalMessageSentCount() {
    return totalMessageSentCount.get();
  }

  public long totalMessageLostCount() {
    return totalMessageLostCount.get();
  }
}
