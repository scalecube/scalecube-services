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
import java.util.function.BiFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.util.annotation.Nullable;

public final class ServiceMessageCodec {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMessageCodec.class);

  private static final HeadersCodec DEFAULT_HEADERS_CODEC = new JdkCodec();

  private final HeadersCodec headersCodec;
  private final Map<String, DataCodec> dataCodecs;

  /** Message codec with default Headers/Data Codecs. */
  public ServiceMessageCodec() {
    this(null, null);
  }

  /**
   * Create instance from headersCodec and set of DataCodec.
   *
   * <p>If codecs are not specified by the user ({@code dataCodecs == null}), DataCodecs obtained
   * through the SPI mechanism are used. If the user sets several DataCodecs for one Content Type,
   * then the last one specified is used. User's DataCodec always override DataCodecs from SPI.
   *
   * <p>Default HeadersCodec is DefaultHeadersCodec. This is lightweight binary codec written on
   * vanilla java.
   *
   * @param headersCodec codec for message headers. Default, {@link JdkCodec}
   * @param dataCodecs codecs for message body. Codec will select by Message Content Type.
   */
  public ServiceMessageCodec(
      @Nullable HeadersCodec headersCodec, @Nullable Collection<DataCodec> dataCodecs) {
    this.headersCodec = headersCodec == null ? DEFAULT_HEADERS_CODEC : headersCodec;
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
        LOGGER.error("Failed to encode data on: {}, cause: {}", message, ex.toString());
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
        LOGGER.error("Failed to encode headers on: {}, cause: {}", message, ex.toString());
        throw new MessageCodecException("Failed to encode headers message q=[" + message + "]", ex);
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
      throw new MessageCodecException(
          "Failed to decode data on message q=" + message.qualifier(), ex);
    }

    return ServiceMessage.from(message).data(data).build();
  }

  /**
   * Get a DataCodec for a content type.
   *
   * @param contentType the content type.
   * @return a DataCodec for the content type or IllegalArgumentException is thrown if non exist
   */
  private DataCodec getDataCodec(String contentType) {
    if (contentType == null) {
      throw new IllegalArgumentException("contentType not specified");
    }
    DataCodec dataCodec = dataCodecs.get(contentType);
    if (dataCodec == null) {
      throw new IllegalArgumentException("DataCodec for '" + contentType + "' not configured");
    }
    return dataCodec;
  }
}
