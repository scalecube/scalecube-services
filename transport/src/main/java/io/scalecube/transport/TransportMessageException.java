package io.scalecube.transport;

/**
 * Thrown to indicate that something happened with application message(in class -- {@code msg}) at the given transport.
 */
public final class TransportMessageException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportMessageException() {
  }

  public TransportMessageException(String message) {
    super(message);
  }

  public TransportMessageException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportMessageException(Throwable cause) {
    super(cause);
  }
}
