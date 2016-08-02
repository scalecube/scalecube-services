package io.scalecube.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler for catching channelActive events on <i>acceptor</i> side. <b>NOTE:</b> this handler must be the
 * first handler after data format handlers in the pipeline.
 */
@ChannelHandler.Sharable
final class AcceptorRegistratorChannelHandler extends ChannelDuplexHandler {
  static final Logger LOGGER = LoggerFactory.getLogger(AcceptorRegistratorChannelHandler.class);

  final ITransportSpi transportSpi;

  AcceptorRegistratorChannelHandler(ITransportSpi transportSpi) {
    this.transportSpi = transportSpi;
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    Channel channel = ctx.channel();
    TransportChannel transport = transportSpi.createAcceptorTransportChannel(channel);
    LOGGER.debug("Registered acceptor: {}", transport);
    super.channelActive(ctx);
  }
}
