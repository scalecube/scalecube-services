package io.scalecube.services.codec;

import java.nio.charset.Charset;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;

public final class ServiceMessageCodec {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);
  private static final String DEFAULT_DATA_FORMAT = "application/json";
  private static final DataCodec dataCodec = DataCodec.getInstance(DEFAULT_DATA_FORMAT);

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
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        ReferenceCountUtil.release(dataBuffer);
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, ex);
        throw new BadRequestException("Failed to encode data on message q=" + message.qualifier());
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        ReferenceCountUtil.release(headersBuffer);
        LOGGER.error("Failed to encode headers on: {}, cause: {}", message, ex);
        throw new BadRequestException("Failed to encode headers on message q=" + message.qualifier());
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
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer.slice())) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        LOGGER.error("Failed to decode message headers: {}, cause: {}",
            headersBuffer.toString(Charset.defaultCharset()), ex);
        throw new BadRequestException("Failed to decode message headers {headers=" + headersBuffer.readableBytes()
            + ", data=" + dataBuffer.readableBytes() + "}");
      } finally {
        ReferenceCountUtil.release(headersBuffer);
      }
    }
    return builder.build();
  }
}
