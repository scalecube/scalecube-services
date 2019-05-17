package io.scalecube.services.transport.api;

import io.scalecube.services.ServiceLoaderUtil;
import io.scalecube.services.api.ServiceMessage;
import java.util.function.BiFunction;

@FunctionalInterface
public interface ServiceMessageDataDecoder
    extends BiFunction<ServiceMessage, Class<?>, ServiceMessage> {

  ServiceMessageDataDecoder INSTANCE =
      ServiceLoaderUtil.findFirst(ServiceMessageDataDecoder.class).orElse(null);
}
