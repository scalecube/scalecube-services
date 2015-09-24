package io.servicefabric.transport;

/**
 * Thrown to indicate that {@link ITransport} can't return transport client is asking for.
 */
public final class TransportNotFoundException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportNotFoundException() {
  }

  public TransportNotFoundException(String message) {
    super(message);
  }

  public TransportNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportNotFoundException(Throwable cause) {
    super(cause);
  }
}
