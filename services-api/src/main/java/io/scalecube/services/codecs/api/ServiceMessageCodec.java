package io.scalecube.services.codecs.api;

import io.scalecube.services.api.ServiceMessage;


public interface ServiceMessageCodec<T> {

  T encodeMessage(ServiceMessage message);

  ServiceMessage decodeMessage(T payload);

}
