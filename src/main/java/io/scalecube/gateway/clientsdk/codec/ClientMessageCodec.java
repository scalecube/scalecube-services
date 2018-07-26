package io.scalecube.gateway.clientsdk.codec;

import io.scalecube.gateway.clientsdk.ClientMessage;
import io.scalecube.gateway.clientsdk.ErrorData;
import io.scalecube.gateway.clientsdk.exceptions.ExceptionProcessor;
import io.scalecube.gateway.clientsdk.exceptions.MessageCodecException;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;

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

public final class ClientMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClientMessageCodec.class);

  private final HeadersCodec headersCodec;
  private final DataCodec dataCodec;

  public ClientMessageCodec(HeadersCodec headersCodec, DataCodec dataCodec) {
    this.headersCodec = headersCodec;
    this.dataCodec = dataCodec;
  }

  public <T> T encodeAndTransform(ClientMessage message, BiFunction<ByteBuf, ByteBuf, T> transformer)
      throws MessageCodecException {
    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.hasData(ByteBuf.class)) {
      dataBuffer = message.data();
    } else if (message.hasData()) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
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

  public ClientMessage decode(ByteBuf dataBuffer, ByteBuf headersBuffer) throws MessageCodecException {
    ClientMessage.Builder builder = ClientMessage.builder();
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

  public ClientMessage decodeData(ClientMessage message, Class<?> dataType) throws MessageCodecException {
    if (!message.hasData(ByteBuf.class) || dataType == null) {
      return message;
    }

    Object data;
    Class<?> targetType = ExceptionProcessor.isError(message.qualifier()) ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer.slice())) {
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode data on: {}, cause: {}, data buffer: {}",
          message, ex, dataBuffer.toString(StandardCharsets.UTF_8));
      throw new MessageCodecException("Failed to decode data on message q=" + message.qualifier(), ex);
    } finally {
      ReferenceCountUtil.safeRelease(dataBuffer);
    }

    if (targetType == ErrorData.class) {
      throw ExceptionProcessor.toException(
          message.qualifier(),
          ((ErrorData) data).getErrorCode(),
          ((ErrorData) data).getErrorMessage());
    }

    return ClientMessage.from(message).data(data).build();
  }
}
