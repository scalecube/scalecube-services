package io.scalecube.services.gateway.client;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.MessageCodecException;
import java.lang.reflect.Type;

public interface GatewayClientCodec {

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
   * Encodes {@link ServiceMessage}.
   *
   * @param message message to encode
   * @return encoded message
   */
  ByteBuf encode(ServiceMessage message);

  /**
   * Decodes {@link ServiceMessage} object from {@link ByteBuf}.
   *
   * @param byteBuf message to decode
   * @return decoded message represented by {@link ServiceMessage}
   */
  ServiceMessage decode(ByteBuf byteBuf);
}
