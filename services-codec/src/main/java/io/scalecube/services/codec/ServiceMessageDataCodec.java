package io.scalecube.services.codec;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.ExceptionProcessor;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

public final class ServiceMessageDataCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageDataCodec.class);

  private static final String DEFAULT_DATA_FORMAT = "application/json";

  public static final DataCodec dataCodec = DataCodec.getInstance(DEFAULT_DATA_FORMAT);

  public ServiceMessage encode(ServiceMessage message) {
    if (message.hasData(ByteBuf.class)) {
      return message;
    } else if (message.hasData()) {
      ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        dataCodec.encode(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        ReferenceCountUtil.release(buffer);
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, ex);
        throw new BadRequestException("Failed to encode data on message q=" + message.qualifier());
      }
    }
    return message;
  }

  public ServiceMessage decode(ServiceMessage message, Class<?> type) {
    if (!message.hasData(ByteBuf.class) || type == null) {
      return message;
    }

    Object data;
    Class<?> targetType = ExceptionProcessor.isError(message) ? ErrorData.class : type;

    try (ByteBufInputStream inputStream = new ByteBufInputStream(((ByteBuf) message.data()).slice())) {
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      LOGGER.error("Failed to decode data on: {}, data buffer: {}, cause: {}",
          message, ex, ((ByteBuf) message.data()).toString(Charset.defaultCharset()));
      throw new BadRequestException("Failed to decode data on message q=" + message.qualifier());
    } finally {
      ReferenceCountUtil.release(message.data());
    }

    if (targetType == ErrorData.class) {
      throw ExceptionProcessor.toException(message.qualifier(), (ErrorData) data);
    }
    return ServiceMessage.from(message).data(data).build();
  }
}
