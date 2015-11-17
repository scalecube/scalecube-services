package io.scalecube.transport;

/** Thrown to indicated something unrecoverable happened with transport. */
public final class TransportBrokenException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportBrokenException() {
  }

  public TransportBrokenException(String message) {
    super(message);
  }

  public TransportBrokenException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportBrokenException(Throwable cause) {
    super(cause);
  }

}
