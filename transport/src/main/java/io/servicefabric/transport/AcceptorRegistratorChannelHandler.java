package io.servicefabric.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler for catching 'channel active' events on 'acceptor' side.
 * <p/>
 * <b>NOTE:</b> this handler must be the first handler after data format handlers in the pipeline.
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
    TransportChannel transport = transportSpi.createAcceptor(channel);
    channel.attr(TransportChannel.ATTR_TRANSPORT).set(transport);
    LOGGER.debug("Registered acceptor: {}", transport);
    super.channelActive(ctx);
  }
}
