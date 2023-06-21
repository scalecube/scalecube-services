package io.scalecube.services.gateway.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import java.lang.reflect.Type;

/**
 * Describes encoding/decoding operations for {@link ServiceMessage} to/from {@link T} type.
 *
 * @param <T> represents source or result for decoding or encoding operations respectively
 */
public interface GatewayClientCodec<T> {

  /**
   * Data decoder function.
   *
   * @param message client message.
   * @param dataType data type class.
   * @return client message object.
   * @throws MessageCodecException in case if data decoding fails.
   */
  default ServiceMessage decodeData(ServiceMessage message, Type dataType)
      throws MessageCodecException {
    return ServiceMessageCodec.decodeData(message, dataType);
  }

  /**
   * Encodes {@link ServiceMessage} to {@link T} type.
   *
   * @param message client message to encode
   * @return encoded message represented by {@link T} type
   */
  T encode(ServiceMessage message);

  /**
   * Decodes message represented by {@link T} type to {@link ServiceMessage} object.
   *
   * @param encodedMessage message to decode
   * @return decoded message represented by {@link ServiceMessage} type
   */
  ServiceMessage decode(T encodedMessage);
}
