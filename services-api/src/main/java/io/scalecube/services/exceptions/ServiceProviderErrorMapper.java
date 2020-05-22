package io.scalecube.services.exceptions;

import io.scalecube.services.api.ServiceMessage;

@FunctionalInterface
public interface ServiceProviderErrorMapper {

  /**
   * Maps an exception to a {@link ServiceMessage}.
   *
   * @param qualifier original qualifier.
   * @param throwable the exception to map to a service message.
   * @return a service message mapped from the supplied exception.
   */
  ServiceMessage toMessage(String qualifier, Throwable throwable);
}
