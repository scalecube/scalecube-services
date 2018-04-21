package io.scalecube.services.transport.rsocket;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import io.rsocket.Payload;

public class RSocketJsonPayloadCodec implements ServiceMessageCodec<Payload> {

  @Override
  public String contentType() {
    return "application/json";
  }
  
  @Override
  public Payload encodeMessage(ServiceMessage message) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage decodeMessage(Payload payload) {
    
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
