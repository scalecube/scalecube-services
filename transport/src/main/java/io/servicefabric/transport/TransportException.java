package io.servicefabric.transport;

/** Generic transport exception. */
public class TransportException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  public TransportException() {
    super();
  }

  public TransportException(String message) {
    super(message);
  }

  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportException(Throwable cause) {
    super(cause);
  }
}
