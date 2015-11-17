package io.scalecube.transport;

/** Thrown to indicate that some issue happened at handshake process between two transport endpoints. */
public final class TransportHandshakeException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportHandshakeException() {
  }

  public TransportHandshakeException(String message) {
    super(message);
  }

  public TransportHandshakeException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportHandshakeException(Throwable cause) {
    super(cause);
  }
}
