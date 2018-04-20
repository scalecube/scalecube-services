package io.scalecube.services.transport;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

public class DummyStringCodec implements ServiceMessageCodec<String> {

  @Override
  public String contentType() {
    return "application/text";
  }

  
  @Override
  public String encodeMessage(ServiceMessage message) {
    return message.data();
  }

  @Override
  public ServiceMessage encodeData(ServiceMessage message) {
    return message;
  }

  @Override
  public ServiceMessage decodeMessage(String payload) {
    return ServiceMessage.builder().data(payload).build();
  }

  @Override
  public ServiceMessage decodeData(ServiceMessage message, Class<?> requestType) {
    return message;
  }


}
