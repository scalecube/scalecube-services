package io.scalecube.services.exceptions;

import java.io.IOException;

public class ConnectionClosedException extends IOException {

  public ConnectionClosedException(String message) {
    super(message);
  }

  @Override
  public synchronized Throwable fillInStackTrace() {
    return this;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{errorMessage=" + getMessage() + '}';
  }
}
