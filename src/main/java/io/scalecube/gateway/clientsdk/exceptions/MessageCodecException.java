package io.scalecube.gateway.clientsdk.exceptions;

public class MessageCodecException extends RuntimeException {

  public MessageCodecException() {}

  public MessageCodecException(String message) {
    super(message);
  }

  public MessageCodecException(String message, Throwable cause) {
    super(message, cause);
  }

  public MessageCodecException(Throwable cause) {
    super(cause);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{errorMessage=" + getMessage() + '}';
  }
}
