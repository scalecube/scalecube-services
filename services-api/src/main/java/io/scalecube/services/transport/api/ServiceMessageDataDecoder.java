package io.scalecube.services.transport.api;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.utils.ServiceLoaderUtil;
import java.lang.reflect.Type;
import java.util.function.BiFunction;

@FunctionalInterface
public interface ServiceMessageDataDecoder
    extends BiFunction<ServiceMessage, Type, ServiceMessage> {

  ServiceMessageDataDecoder INSTANCE =
      ServiceLoaderUtil.findFirst(ServiceMessageDataDecoder.class).orElse(null);
}
