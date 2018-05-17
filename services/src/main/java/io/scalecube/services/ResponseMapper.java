package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

@FunctionalInterface
public interface ResponseMapper {

  ServiceMessage apply(ServiceMessage response, Class<?> responseType);
}
