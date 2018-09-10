package io.scalecube.gateway.clientsdk.http;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.gateway.clientsdk.ClientCodec;
import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.exceptions.MessageCodecException;
import io.scalecube.services.codec.DataCodec;

public class HttpClientCodec implements ClientCodec<ByteBuf> {

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
    ByteBuf dataBuffer = ByteBufAllocator.DEFAULT.buffer();

    try {
      dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
    } catch (Throwable t) {
      LOGGER.error("Failed to encode data on: {}, cause: {}", message, t);
      ReferenceCountUtil.safeRelease(dataBuffer);
      throw new MessageCodecException(
          "Failed to encode data on message q=" + message.qualifier(), t);
    }

    return dataBuffer;
  }

  @Override
  public ClientMessage decode(ByteBuf encodedMessage) {
    return ClientMessage.builder().data(encodedMessage).build();
  }
}
