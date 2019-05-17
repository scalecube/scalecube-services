package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;

public class ServiceMessageByteBufDataDecoder implements ServiceMessageDataDecoder {

  @Override
  public ServiceMessage apply(ServiceMessage message, Class<?> dataType) {
    return ServiceMessageCodec.decodeData(message, dataType);
  }
}
