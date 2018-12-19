package io.scalecube.services.gateway.clientsdk.exceptions.mappers;

import io.scalecube.services.gateway.clientsdk.ClientMessage;

@FunctionalInterface
public interface ClientErrorMapper {

  /**
   * Maps client message to an exception.
   *
   * @param message the message to map to an exception.
   * @return an exception mapped from qualifier and error data.
   */
  Throwable toError(ClientMessage message);
}
