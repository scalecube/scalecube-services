package io.scalecube.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ReferenceCountUtil;
import io.scalecube.gateway.clientsdk.exceptions.MessageCodecException;
import io.scalecube.services.codec.DataCodec;

public final class HttpClientCodec implements ClientCodec<ByteBuf> {

  private final DataCodec dataCodec;

  /**
   * Constructor for codec which encode/decode client message to/from {@link ByteBuf}.
   *
   * @param dataCodec data message codec.
   */
  public HttpClientCodec(DataCodec dataCodec) {
    this.dataCodec = dataCodec;
  }

  @Override
  public DataCodec getDataCodec() {
    return dataCodec;
  }

  @Override
  public ByteBuf encode(ClientMessage message) {
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
  public ClientMessage decode(ByteBuf encodedMessage) {
    return ClientMessage.builder().data(encodedMessage).build();
  }
}
