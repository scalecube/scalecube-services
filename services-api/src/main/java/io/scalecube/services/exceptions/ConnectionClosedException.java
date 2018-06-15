package io.scalecube.services.exceptions;

public class ConnectionClosedException extends ServiceUnavailableException {

  public ConnectionClosedException(String message) {
    super(message);
  }

  public ConnectionClosedException(int errorCode, String message) {
    super(errorCode, message);
  }
}
