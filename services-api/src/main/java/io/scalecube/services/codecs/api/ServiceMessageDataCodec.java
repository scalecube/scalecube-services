package io.scalecube.services.codecs.api;

import io.scalecube.services.api.ServiceMessage;

public interface ServiceMessageDataCodec extends ServiceMessageContentType {

  ServiceMessage encodeData(ServiceMessage message);

  ServiceMessage decodeData(ServiceMessage message, Class<?> requestType);
  
}
