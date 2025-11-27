package io.scalecube.services.transport.rsocket;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.Type;

public class ServiceMessageByteBufDataDecoder implements ServiceMessageDataDecoder {

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Type dataType) {
    return ServiceMessageCodec.decodeData(message, dataType, false);
  }

  @Override
  public ServiceMessage copyData(ServiceMessage message, Type dataType) {
    return ServiceMessageCodec.decodeData(message, dataType, true);
  }
}
