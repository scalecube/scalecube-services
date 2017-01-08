package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

@ChannelHandler.Sharable
final class NetworkEmulatorHandler extends ChannelOutboundHandlerAdapter {

  private static final Logger LOGGER = LoggerFactory.getLogger(NetworkEmulatorHandler.class);

  private final NetworkEmulator networkEmulator;

  public NetworkEmulatorHandler(NetworkEmulator networkEmulator) {
    this.networkEmulator = networkEmulator;
  }

  @Override
  public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
    NetworkLinkSettings networkSettings = resolveNetworkSettings(ctx.channel());
    networkEmulator.incrementMessageSentCount();

    // Emulate message loss
    boolean isLost = networkSettings.evaluateLoss();
    if (isLost) {
      networkEmulator.incrementMessageLostCount();
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

  private NetworkLinkSettings resolveNetworkSettings(Channel channel) {
    InetSocketAddress remoteSocketAddress = (InetSocketAddress) channel.remoteAddress();
    return networkEmulator.getLinkSettings(remoteSocketAddress);
  }
}
