package io.scalecube.services.transport.rsocket;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import io.rsocket.Payload;

public class RSocketPayloadCodec implements ServiceMessageCodec<Payload> {

  @Override
  public Payload encodeMessage(ServiceMessage message) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage decodeMessage(Payload payload) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class<?> dataType) {
    // TODO Auto-generated method stub
    return null;
  }


}
