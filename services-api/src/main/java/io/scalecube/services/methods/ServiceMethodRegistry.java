package io.scalecube.services.methods;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;

public interface ServiceMethodRegistry {

  void registerService(
      Object serviceInstance,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator authenticator);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);
}
