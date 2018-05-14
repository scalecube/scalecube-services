package io.scalecube.services.codec;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.util.ReferenceCountUtil;

import java.util.Optional;

public final class ServiceMessageDataCodec {

  private static final String DEFAULT_DATA_FORMAT = "application/json";

  private static final String ERROR_DATA_ENCODE_FAILED = "Failed to serialize data on %s, cause: %s";
  private static final String ERROR_DATA_DECODE_FAILED = "Failed to deserialize data on %s, cause: %s";

  public ServiceMessage encode(ServiceMessage message) {
    if (message.hasData()) {
      ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        dataCodec.encode(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        ReferenceCountUtil.release(buffer);
        throw new BadRequestException(String.format(ERROR_DATA_ENCODE_FAILED, message, ex));
      }
    }
    return message;
  }

  public ServiceMessage decode(ServiceMessage message, Class type) {
    if (message.hasData(ByteBuf.class)) {
      try (ByteBufInputStream inputStream = new ByteBufInputStream(message.data(), true)) {
        String contentType = Optional.ofNullable(message.dataFormat()).orElse(DEFAULT_DATA_FORMAT);
        DataCodec dataCodec = DataCodec.getInstance(contentType);
        return ServiceMessage.from(message).data(dataCodec.decode(inputStream, type)).build();
      } catch (Throwable ex) {
        throw new BadRequestException(String.format(ERROR_DATA_DECODE_FAILED, message, ex));
      }
    }
    return message;
  }
}
