package io.scalecube.services.methods;

import io.scalecube.services.exceptions.mappers.ServiceProviderErrorMapper;

public interface ServiceMethodRegistry {

  void registerService(Object serviceInstance, ServiceProviderErrorMapper errorMapper);

  boolean containsInvoker(String qualifier);

  ServiceMethodInvoker getInvoker(String qualifier);
}
