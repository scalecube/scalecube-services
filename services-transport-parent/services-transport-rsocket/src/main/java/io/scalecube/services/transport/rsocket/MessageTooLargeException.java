package io.scalecube.services.transport.rsocket;

import io.scalecube.services.exceptions.InternalServiceException;

/**
 * Thrown when a message exceeds the configured maximum message size while being encoded. It carries
 * error code {@code 413} ("payload too large") and is an {@link InternalServiceException} (error
 * type {@code 500} on the wire), so it propagates to the caller as a normal service error — with the
 * same {@code 413} code whether it originates on the responder (oversized response) or the client
 * (oversized request) — rather than as a transport-level frame failure.
 */
public class MessageTooLargeException extends InternalServiceException {

  public static final int ERROR_CODE = 413;

  public MessageTooLargeException(String message) {
    super(ERROR_CODE, message);
  }
}
