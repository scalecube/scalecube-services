package io.scalecube.services.exceptions.mappers;

import io.scalecube.services.api.ServiceMessage;

@FunctionalInterface
public interface ServiceProviderErrorMapper {

  /**
   * Maps an exception to a {@link ServiceMessage}.
   *
   * @param throwable the exception to map to a service message.
   * @return a service message mapped from the supplied exception.
   */
  ServiceMessage toMessage(Throwable throwable);
}
