package io.scalecube.services.exceptions;

public class MessageCodecException extends RuntimeException {

  public MessageCodecException(String message, Throwable cause) {
    super(message, cause);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{errorMessage=" + getMessage() + '}';
  }
}
