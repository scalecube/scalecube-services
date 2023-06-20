package io.scalecube.services.transport.rsocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;

public class ServiceMessageByteBufDataDecoder implements ServiceMessageDataDecoder {

  @Override
  public ServiceMessage apply(ServiceMessage message, Class<?> dataType) {
    return ServiceMessageCodec.decodeData(message, dataType);
  }
}
