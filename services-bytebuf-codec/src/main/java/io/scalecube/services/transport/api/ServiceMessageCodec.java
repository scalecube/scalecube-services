package io.scalecube.services.transport.api;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toMap;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);

  private final HeadersCodec headersCodec;
  private final Map<String, DataCodec> dataCodecs;

  /**
   * Message codec with default Headers/Data Codecs.
   *
   * @see HeadersCodec#DEFAULT_INSTANCE
   * @see DataCodec#getAllInstances()
   */
  public ServiceMessageCodec() {
    this(null, null);
  }

  /**
   * Constructor. Creates instance out of {@link HeadersCodec} instance and {@link DataCodec}
   * collection.
   *
   * <p><b>NOTE:</b> If client set several data codecs for one content type (see what's content type
   * here: {@link DataCodec#contentType()}), then the last one specified will be used. Client's
   * collection of data codes override data codecs from SPI.
   *
   * @param headersCodec codec for service message headers; optional, if not set then {@link
   *     JdkCodec} will be used.
   * @param dataCodecs codecs for service message data; optional, if not set then {@link
   *     DataCodec#INSTANCES} will be used.
   */
  public ServiceMessageCodec(HeadersCodec headersCodec, Collection<DataCodec> dataCodecs) {
    this.headersCodec = headersCodec == null ? HeadersCodec.DEFAULT_INSTANCE : headersCodec;
    Map<String, DataCodec> defaultCodecs = DataCodec.INSTANCES;
    if (dataCodecs == null) {
      this.dataCodecs = defaultCodecs;
    } else {
      this.dataCodecs =
          dataCodecs.stream()
              .collect(
                  collectingAndThen(
                      toMap(DataCodec::contentType, identity(), (c1, c2) -> c2),
                      usersCodec -> {
                        Map<String, DataCodec> buffer = new HashMap<>(defaultCodecs);
                        buffer.putAll(usersCodec);
                        return Collections.unmodifiableMap(buffer);
                      }));
    }
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
        DataCodec dataCodec = getDataCodec(message.dataFormatOrDefault());
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(dataBuffer);
        LOGGER.error(
            "Failed to encode service message data on: {}, cause: {}", message, ex.toString());
        throw new MessageCodecException("Failed to encode service message data", ex);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = ByteBufAllocator.DEFAULT.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(headersBuffer);
        ReferenceCountUtil.safestRelease(dataBuffer); // release data buf as well
        LOGGER.error(
            "Failed to encode service message headers on: {}, cause: {}", message, ex.toString());
        throw new MessageCodecException("Failed to encode service message headers", ex);
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
      try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer, true)) {
        builder.headers(headersCodec.decode(stream));
      } catch (Throwable ex) {
        ReferenceCountUtil.safestRelease(dataBuffer); // release data buf as well
        throw new MessageCodecException("Failed to decode service message headers", ex);
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
  public static ServiceMessage decodeData(ServiceMessage message, Type dataType)
      throws MessageCodecException {
    if (dataType == null
        || !message.hasData(ByteBuf.class)
        || ((ByteBuf) message.data()).readableBytes() == 0) {
      return message;
    }

    Object data;
    Type targetType = message.isError() ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer, true)) {
      DataCodec dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
      data = dataCodec.decode(inputStream, targetType);
    } catch (Throwable ex) {
      throw new MessageCodecException("Failed to decode service message data", ex);
    }

    return ServiceMessage.from(message).data(data).build();
  }

  private DataCodec getDataCodec(String contentType) {
    Objects.requireNonNull(contentType, "contentType");
    DataCodec dataCodec = dataCodecs.get(contentType);
    return Objects.requireNonNull(dataCodec, "dataCodec");
  }
}
