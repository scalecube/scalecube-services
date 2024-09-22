package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import java.util.ServiceLoader;
import java.util.function.BiFunction;
import java.util.stream.StreamSupport;

@FunctionalInterface
public interface ServiceMessageDataDecoder
    extends BiFunction<ServiceMessage, Class<?>, ServiceMessage> {

  ServiceMessageDataDecoder INSTANCE =
      StreamSupport.stream(ServiceLoader.load(ServiceMessageDataDecoder.class).spliterator(), false)
          .findFirst()
          .orElse(null);
}
