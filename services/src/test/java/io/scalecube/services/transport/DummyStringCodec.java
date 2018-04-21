package io.scalecube.services.transport;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

public class DummyStringCodec implements ServiceMessageCodec<String> {

  @Override
  public String contentType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String encodeMessage(ServiceMessage message) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage decodeMessage(String payload) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class<?> requestType) {
    // TODO Auto-generated method stub
    return null;
  }

}
