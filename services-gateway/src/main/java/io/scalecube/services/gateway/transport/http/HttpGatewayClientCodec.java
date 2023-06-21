package io.scalecube.services.gateway.transport.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import io.scalecube.services.gateway.ReferenceCountUtil;
import io.scalecube.services.gateway.transport.GatewayClientCodec;
import io.scalecube.services.transport.api.DataCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class HttpGatewayClientCodec implements GatewayClientCodec<ByteBuf> {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpGatewayClientCodec.class);

  private final DataCodec dataCodec;

  /**
   * Constructor for codec which encode/decode client message to/from {@link ByteBuf}.
   *
   * @param dataCodec data message codec.
   */
  public HttpGatewayClientCodec(DataCodec dataCodec) {
    this.dataCodec = dataCodec;
  }

  @Override
  public ByteBuf encode(ServiceMessage message) {
    ByteBuf content;

    if (message.hasData(ByteBuf.class)) {
      content = message.data();
    } else {
      content = ByteBufAllocator.DEFAULT.buffer();
      try {
        dataCodec.encode(new ByteBufOutputStream(content), message.data());
      } catch (Throwable t) {
        ReferenceCountUtil.safestRelease(content);
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, t);
        throw new MessageCodecException(
            "Failed to encode data on message q=" + message.qualifier(), t);
      }
    }

    return content;
  }

  @Override
  public ServiceMessage decode(ByteBuf encodedMessage) {
    return ServiceMessage.builder().data(encodedMessage).build();
  }
}
