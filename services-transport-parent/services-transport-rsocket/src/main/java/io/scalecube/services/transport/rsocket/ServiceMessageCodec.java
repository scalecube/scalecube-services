package io.scalecube.services.transport.rsocket;

import static io.scalecube.services.transport.rsocket.ReferenceCountUtil.safestRelease;
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
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.api.JdkCodec;
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
   * @param headersCodec codec for service message headers (optional), if not set then {@link
   *     JdkCodec} will be used.
   * @param dataCodecs codecs for service message data (optional), if not set then {@link
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
    final var bufAllocator = ByteBufAllocator.DEFAULT;
    ByteBuf dataBuffer = Unpooled.EMPTY_BUFFER;
    ByteBuf headersBuffer = Unpooled.EMPTY_BUFFER;

    if (message.hasData(ByteBuf.class)) {
      dataBuffer = message.data();
    } else if (message.hasData(byte[].class)) {
      final var bytes = (byte[]) message.data();
      dataBuffer = bufAllocator.buffer(bytes.length);
      dataBuffer.writeBytes(bytes);
    } else if (message.hasData()) {
      dataBuffer = bufAllocator.buffer();
      try {
        DataCodec dataCodec = getDataCodec(message.dataFormatOrDefault());
        dataCodec.encode(new ByteBufOutputStream(dataBuffer), message.data());
      } catch (Throwable ex) {
        safestRelease(dataBuffer);
        LOGGER.error(
            "Failed to encode service message data on: {}, cause: {}", message, ex.toString());
        throw new MessageCodecException("Failed to encode service message data", ex);
      }
    }

    if (!message.headers().isEmpty()) {
      headersBuffer = bufAllocator.buffer();
      try {
        headersCodec.encode(new ByteBufOutputStream(headersBuffer), message.headers());
      } catch (Throwable ex) {
        safestRelease(headersBuffer);
        safestRelease(dataBuffer); // release data buf as well
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

    try (ByteBufInputStream stream = new ByteBufInputStream(headersBuffer, true)) {
      builder.headers(headersCodec.decode(stream)).data(dataBuffer);
    } catch (Throwable ex) {
      safestRelease(dataBuffer); // release data buf as well
      throw new MessageCodecException("Failed to decode service message headers", ex);
    }

    return builder.build();
  }

  /**
   * Decode message.
   *
   * @param message the original message (with {@link ByteBuf} data)
   * @param dataType the type of the data
   * @param copyOnDecode whether to copy the buffer before decoding
   * @return a new Service message that upon {@link ServiceMessage#data()} returns the actual data
   *     (of type data type)
   * @throws MessageCodecException when decode fails
   */
  public static ServiceMessage decodeData(
      ServiceMessage message, Type dataType, boolean copyOnDecode) throws MessageCodecException {
    if (dataType == null
        || dataType == ByteBuf.class
        || message.data() == null
        || !(message.data() instanceof ByteBuf)) {
      return message;
    }

    final ByteBuf dataBuffer = message.data();

    if (dataType == byte[].class) {
      final var bytes = new byte[dataBuffer.readableBytes()];
      dataBuffer.getBytes(dataBuffer.readerIndex(), bytes);
      if (!copyOnDecode) {
        safestRelease(dataBuffer);
      }
      return ServiceMessage.from(message).data(bytes).build();
    }

    if (dataBuffer.readableBytes() == 0) {
      return ServiceMessage.from(message).data(null).build();
    }

    try (ByteBufInputStream inputStream =
        copyOnDecode
            ? new ByteBufInputStream(Unpooled.copiedBuffer(dataBuffer))
            : new ByteBufInputStream(dataBuffer, true)) {
      final var targetType = message.isError() ? ErrorData.class : dataType;
      final var dataCodec = DataCodec.getInstance(message.dataFormatOrDefault());
      final var decodedData = dataCodec.decode(inputStream, targetType);
      return ServiceMessage.from(message).data(decodedData).build();
    } catch (Throwable ex) {
      throw new MessageCodecException("Failed to decode service message data", ex);
    }
  }

  private DataCodec getDataCodec(String contentType) {
    Objects.requireNonNull(contentType, "contentType");
    DataCodec dataCodec = dataCodecs.get(contentType);
    return Objects.requireNonNull(dataCodec, "dataCodec");
  }
}
