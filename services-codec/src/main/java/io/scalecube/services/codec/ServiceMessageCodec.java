package io.scalecube.services.codec;

import io.scalecube.services.api.ServiceMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public final class ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);

  private static final String DEFAULT_DATA_FORMAT = "application/json";

  private final HeadersCodec headersCodec;

  public ServiceMessageCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
  }

  public ByteBuf[] encodeMessage(ServiceMessage message) {
    ByteBuf[] bufs = new ByteBuf[2];

    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.data() instanceof ByteBuf) { // has data ?
      dataBuffer = message.data(); // ok so use it as is
    } else if (message.data() != null) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
        ReferenceCountUtil.release(dataBuffer);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(headersBuffer);
      }
    }

    bufs[0] = dataBuffer;
    bufs[1] = headersBuffer;

    return bufs;
  }

  public ServiceMessage decodeMessage(ByteBuf dataBuffer, ByteBuf headersBuffer) {
    ServiceMessage.Builder builder = ServiceMessage.builder();
    if (dataBuffer.isReadable()) {
      builder.data(dataBuffer);
    }
    if (headersBuffer.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer, true)) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return builder.build();
  }
}
