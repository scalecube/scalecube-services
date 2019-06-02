package io.scalecube.services.examples.exceptions;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;

public class ServiceAProviderErrorMapper implements ServiceProviderErrorMapper {

  @Override
  public ServiceMessage toMessage(Throwable throwable) {
    // implement service mapping logic
    if (throwable instanceof ServiceAException) {
      ServiceAException e = (ServiceAException) throwable;
      return ServiceMessage.error(BadRequestException.ERROR_TYPE, e.code(), e.getMessage());
    }

    // or delegate it to default mapper
    return DefaultErrorMapper.INSTANCE.toMessage(throwable);
  }
}
