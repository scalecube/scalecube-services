package io.scalecube.services.exceptions.mappers;

import io.scalecube.services.api.ServiceMessage;

@FunctionalInterface
public interface ServiceClientErrorMapper {

  /**
   * Maps service message to an exception.
   *
   * @param message the message to map to an exception.
   * @return an exception mapped from qualifier and error data.
   */
  Throwable toError(ServiceMessage message);
}
