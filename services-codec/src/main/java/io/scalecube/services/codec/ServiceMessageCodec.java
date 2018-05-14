package io.scalecube.services.codec;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import java.util.Optional;
import java.util.function.BiFunction;

public final class ServiceMessageCodec {

  private static final String DEFAULT_DATA_FORMAT = "application/json";

  private static final String ERROR_DATA_ENCODE_FAILED = "Failed to serialize data on %s, cause: %s";
  private static final String ERROR_HEADERS_ENCODE_FAILED = "Failed to serialize headers on %s, cause: %s";
  private static final String ERROR_HEADERS_DECODE_FAILED = "Failed to deserialize headers, cause: %s";

  private final HeadersCodec headersCodec;

  public ServiceMessageCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
  }

  public <T> T encodeAndTransform(ServiceMessage message, BiFunction<ByteBuf, ByteBuf, T> transformer) {
    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.hasData(ByteBuf.class)) {
      dataBuffer = message.data();
    } else if (message.hasData()) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        ReferenceCountUtil.release(dataBuffer);
        throw new BadRequestException(String.format(ERROR_DATA_ENCODE_FAILED, message, ex));
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        ReferenceCountUtil.release(headersBuffer);
        throw new BadRequestException(String.format(ERROR_HEADERS_ENCODE_FAILED, message, ex));
      }
    }

    return transformer.apply(dataBuffer, headersBuffer);
  }

  public ServiceMessage decode(ByteBuf dataBuffer, ByteBuf headersBuffer) {
    ServiceMessage.Builder builder = ServiceMessage.builder();
    if (dataBuffer.isReadable()) {
      builder.data(dataBuffer);
    }
    if (headersBuffer.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer, true)) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        throw new BadRequestException(String.format(ERROR_HEADERS_DECODE_FAILED, ex));
      }
    }
    return builder.build();
  }
}
