package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import java.lang.reflect.Type;

public class ServiceMessageByteBufDataDecoder implements ServiceMessageDataDecoder {

  @Override
  public ServiceMessage apply(ServiceMessage message, Type dataType) {
    return ServiceMessageCodec.decodeData(message, dataType);
  }
}
