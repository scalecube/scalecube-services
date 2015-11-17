package io.scalecube.transport;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Channel handler for getting message traffic. Activated when connection established/accepted and handshake passed.
 * <p/>
 * <b>NOTE:</b> in the pipeline this handler must be set just right before {@link ExceptionCaughtChannelHandler}.
 */
@ChannelHandler.Sharable
final class MessageReceiverChannelHandler extends ChannelInboundHandlerAdapter {
  static final Logger LOGGER = LoggerFactory.getLogger(MessageReceiverChannelHandler.class);

  final ITransportSpi transportSpi;

  MessageReceiverChannelHandler(ITransportSpi transportSpi) {
    this.transportSpi = transportSpi;
  }

  /**
   * Publish {@code msg} on the TransportFactory subject.
   */
  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    TransportChannel transportChannel = TransportChannel.from(ctx.channel());
    Message message = (Message) msg;
    message.setSender(transportChannel.remoteEndpoint());
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received {}", message);
    }
    transportSpi.onMessage(message);
  }
}
