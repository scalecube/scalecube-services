package io.scalecube.transport;

/** Thrown to indicate that transport is already closed and operations with this transport can't progress anymore. */
public final class TransportClosedException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportClosedException() {
  }

  public TransportClosedException(String message) {
    super(message);
  }

  public TransportClosedException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportClosedException(Throwable cause) {
    super(cause);
  }
}
