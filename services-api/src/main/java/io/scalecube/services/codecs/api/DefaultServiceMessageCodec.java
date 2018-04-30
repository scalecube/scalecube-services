package io.scalecube.services.codecs.api;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessage.Builder;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultServiceMessageCodec implements ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServiceMessageCodec.class);

  private final ServiceMessageCodecInterface codecInterface;

  public DefaultServiceMessageCodec(ServiceMessageCodecInterface codecInterface) {
    this.codecInterface = codecInterface;
  }

  @Override
  public ByteBuf[] encodeMessage(ServiceMessage message) {
    ByteBuf[] bufs = new ByteBuf[2];

    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.data() instanceof ByteBuf) { // has data ?
      dataBuffer = message.data(); // ok so use it as is
    } else if (message.data() != null) {
      dataBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        codecInterface.writeBody(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
        ReferenceCountUtil.release(dataBuffer);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        codecInterface.writeHeaders(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(headersBuffer);
      }
    }

    bufs[0] = dataBuffer;
    bufs[1] = headersBuffer;

    return bufs;
  }

  @Override
  public ServiceMessage decodeMessage(ByteBuf dataBuffer, ByteBuf headersBuffer) {
    Builder builder = ServiceMessage.builder();
    if (dataBuffer.isReadable()) {
      builder.data(dataBuffer);
    }
    if (headersBuffer.isReadable()) {
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer, true)) {
        builder.headers(codecInterface.readHeaders(stream));
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return builder.build();
  }

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class type) {
    if (message.data() instanceof ByteBuf) {
      try (ByteBufInputStream inputStream = new ByteBufInputStream(message.data(), true)) {
        return ServiceMessage.from(message).data(codecInterface.readBody(inputStream, type)).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to deserialize data", ex);
      }
    }
    return message;
  }

  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    if (message.data() != null) {
      ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        codecInterface.writeBody(new ByteBufOutputStream(buffer), message.data());
        return ServiceMessage.from(message).data(buffer).build();
      } catch (Throwable ex) {
        LOGGER.error("Failed to serialize data", ex);
        ReferenceCountUtil.release(buffer);
      }
    }
    return message;
  }
}
