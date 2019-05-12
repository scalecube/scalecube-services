package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import java.util.function.BiFunction;

public interface ServiceMethodRegistry {

  void registerService(
      Object serviceInstance,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);
}
