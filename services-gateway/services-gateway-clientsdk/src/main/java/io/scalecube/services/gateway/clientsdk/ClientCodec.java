package io.scalecube.services.gateway.clientsdk;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.scalecube.services.gateway.clientsdk.exceptions.MessageCodecException;
import io.scalecube.services.transport.api.DataCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes encoding/decoding operations for {@link ClientMessage} to/from {@link T} type.
 *
 * @param <T> type which represents source or result for decoding or encoding operations
 *     respectively
 */
public interface ClientCodec<T> {

  Logger LOGGER = LoggerFactory.getLogger(ClientCodec.class);

  /**
   * Data decoder function.
   *
   * @param message client message.
   * @param dataType data type class.
   * @return client message object.
   * @throws MessageCodecException in case if data decoding fails.
   */
  default ClientMessage decodeData(ClientMessage message, Class<?> dataType)
      throws MessageCodecException {
    if (!message.hasData(ByteBuf.class) || dataType == null) {
      return message;
    }

    Object data;
    Class<?> targetType = message.isError() ? ErrorData.class : dataType;

    ByteBuf dataBuffer = message.data();
    try (ByteBufInputStream inputStream = new ByteBufInputStream(dataBuffer, true)) {
      data = getDataCodec().decode(inputStream, targetType);
    } catch (Throwable ex) {
      throw new MessageCodecException(
          "Failed to decode data on message q=" + message.qualifier(), ex);
    }

    return ClientMessage.from(message).data(data).build();
  }

  /**
   * Returns codec which is used to decode data object of {@link ClientMessage}.
   *
   * @see ClientCodec#decodeData(ClientMessage, Class)
   * @return data codec
   */
  DataCodec getDataCodec();

  /**
   * Encodes {@link ClientMessage} to {@link T} type.
   *
   * @param message client message to encode
   * @return encoded message represented by {@link T} type
   */
  T encode(ClientMessage message);

  /**
   * Decodes message represented by {@link T} type to {@link ClientMessage} object.
   *
   * @param encodedMessage message to decode
   * @return decoded message represented by {@link ClientMessage} type
   */
  ClientMessage decode(T encodedMessage);
}
