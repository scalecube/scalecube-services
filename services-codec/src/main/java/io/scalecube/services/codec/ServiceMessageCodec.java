package io.scalecube.services.codec;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.MessageCodecException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.function.BiFunction;

public final class ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);

  private final HeadersCodec headersCodec;

  public ServiceMessageCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
  }

  public <T> T encodeAndTransform(ServiceMessage message, BiFunction<ByteBuf, ByteBuf, T> transformer)
      throws MessageCodecException {
    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.hasData(ByteBuf.class)) {
      dataBuffer = message.data();
    } else if (message.hasData()) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        DataCodec dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        ReferenceCountUtil.safeRelease(dataBuffer);
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, ex);
        throw new MessageCodecException("Failed to encode data on message q=" + message.qualifier(), ex);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        ReferenceCountUtil.safeRelease(headersBuffer);
        LOGGER.error("Failed to encode headers on: {}, cause: {}", message, ex);
        throw new MessageCodecException("Failed to encode headers on message q=" + message.qualifier(), ex);
      }
    }

    return transformer.apply(dataBuffer, headersBuffer);
  }

  public ServiceMessage decode(ByteBuf dataBuffer, ByteBuf headersBuffer) throws MessageCodecException {
    ServiceMessage.Builder builder = ServiceMessage.builder();
    if (dataBuffer.isReadable()) {
      builder.data(dataBuffer);
    }
    if (headersBuffer.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer.slice())) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        LOGGER.error("Failed to decode message headers: {}, cause: {}",
            headersBuffer.toString(StandardCharsets.UTF_8), ex);
        throw new MessageCodecException("Failed to decode message headers", ex);
      } finally {
        ReferenceCountUtil.safeRelease(headersBuffer);
      }
    }
    return builder.build();
  }

  public static ServiceMessage decodeData(ServiceMessage message, Class<?> dataType) throws MessageCodecException {
    if (!message.hasData(ByteBuf.class) || dataType == null) {
      return message;
    }

    Object data;
    Class<?> targetType = ExceptionProcessor.isError(message) ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer.slice())) {
      DataCodec dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode data on: {}, cause: {}, data buffer: {}",
          message, ex, dataBuffer.toString(StandardCharsets.UTF_8));
      throw new MessageCodecException("Failed to decode data on message q=" + message.qualifier(), ex);
    } finally {
      ReferenceCountUtil.safeRelease(dataBuffer);
    }

    if (targetType == ErrorData.class) {
      throw ExceptionProcessor.toException(message.qualifier(), (ErrorData) data);
    }

    return ServiceMessage.from(message).data(data).build();
  }
}
