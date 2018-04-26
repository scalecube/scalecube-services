package io.scalecube.services.codecs.api;

import io.scalecube.services.api.ServiceMessage;


public interface ServiceMessageCodec<T> {

  T encodeMessage(ServiceMessage message);

  ServiceMessage decodeMessage(T payload);

  ServiceMessage encodeData(ServiceMessage message);

  ServiceMessage decodeData(ServiceMessage message, Class<?> requestType);

  String contentType();

}
