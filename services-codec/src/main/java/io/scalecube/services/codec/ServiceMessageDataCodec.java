package io.scalecube.services.codec;

import io.scalecube.services.api.ServiceMessage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public final class ServiceMessageDataCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageDataCodec.class);

  private static final String DEFAULT_DATA_FORMAT = "application/json";

  public ServiceMessage encode(ServiceMessage message) {
    if (message.data() != null) {
      ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        dataCodec.encode(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(buffer);
      }
    }
    return message;
  }

  public ServiceMessage decode(ServiceMessage message, Class type) {
    if (message.data() instanceof ByteBuf) {
      try (ByteBufInputStream inputStream = new ByteBufInputStream(message.data(), true)) {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        return ServiceMessage.from(message).data(dataCodec.decode(inputStream, type)).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return message;
  }
}
