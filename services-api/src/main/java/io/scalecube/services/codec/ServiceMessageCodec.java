package io.scalecube.services.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.exceptions.MessageCodecException;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);

  private final HeadersCodec headersCodec;

  public ServiceMessageCodec(HeadersCodec headersCodec) {
    this.headersCodec = headersCodec;
  }

  /**
   * Encode a message, transform it to T.
   *
   * @param message the message to transform
   * @param transformer a function that accepts data and header {@link ByteBuf} and return the
   *     required T
   * @return the object (transformed message)
   * @throws MessageCodecException when encoding cannot be done.
   */
  public <T> T encodeAndTransform(
      ServiceMessage message, BiFunction<ByteBuf, ByteBuf, T> transformer)
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
        ReferenceCountUtil.safestRelease(dataBuffer);
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, ex);
        throw new MessageCodecException(
            "Failed to encode data on message q=" + message.qualifier(), ex);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(headersBuffer);
        ReferenceCountUtil.safestRelease(dataBuffer); // release data buf as well
        LOGGER.error("Failed to encode headers on: {}, cause: {}", message, ex);
        throw new MessageCodecException(
            "Failed to encode headers on message q=" + message.qualifier(), ex);
      }
    }

    return transformer.apply(dataBuffer, headersBuffer);
  }

  /**
   * Decode buffers.
   *
   * @param dataBuffer the buffer of the data (payload)
   * @param headersBuffer the buffer of the headers
   * @return a new Service message with {@link ByteBuf} data and with parsed headers.
   * @throws MessageCodecException when decode fails
   */
  public ServiceMessage decode(ByteBuf dataBuffer, ByteBuf headersBuffer)
      throws MessageCodecException {
    ServiceMessage.Builder builder = ServiceMessage.builder();

    if (dataBuffer.isReadable()) {
      builder.data(dataBuffer);
    }
    if (headersBuffer.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer.slice(), true)) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(dataBuffer); // release data buf as well
        throw new MessageCodecException("Failed to decode message headers", ex);
      }
    }

    return builder.build();
  }

  /**
   * Decode message.
   *
   * @param message the original message (with {@link ByteBuf} data)
   * @param dataType the type of the data.
   * @return a new Service message that upon {@link ServiceMessage#data()} returns the actual data
   *     (of type data type)
   * @throws MessageCodecException when decode fails
   */
  public static ServiceMessage decodeData(ServiceMessage message, Class<?> dataType)
      throws MessageCodecException {
    if (!message.hasData(ByteBuf.class) || dataType == null) {
      return message;
    }

    Object data;
    Class<?> targetType = ExceptionProcessor.isError(message) ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer.slice(), true)) {
      DataCodec dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      throw new MessageCodecException(
          "Failed to decode data on message q=" + message.qualifier(), ex);
    }

    if (targetType == ErrorData.class) {
      throw ExceptionProcessor.toException(message.qualifier(), (ErrorData) data);
    }

    return ServiceMessage.from(message).data(data).build();
  }
}
