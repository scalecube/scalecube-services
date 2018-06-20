package io.scalecube.services.exceptions;

import java.io.IOException;

public class ConnectionClosedException extends IOException {

  public ConnectionClosedException(String message) {
    super(message);
  }

  public ConnectionClosedException(Throwable cause) {
    super(cause);
  }
}
