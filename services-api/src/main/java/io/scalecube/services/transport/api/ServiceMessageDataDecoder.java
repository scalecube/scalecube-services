package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import java.lang.reflect.Type;
import java.util.ServiceLoader;
import java.util.stream.StreamSupport;

public interface ServiceMessageDataDecoder {

  ServiceMessageDataDecoder INSTANCE =
      StreamSupport.stream(ServiceLoader.load(ServiceMessageDataDecoder.class).spliterator(), false)
          .findFirst()
          .orElse(null);

  ServiceMessage decodeData(ServiceMessage message, Type type);

  ServiceMessage copyData(ServiceMessage message, Type type);
}
