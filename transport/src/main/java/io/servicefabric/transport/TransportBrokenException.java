package io.servicefabric.transport;

/** Thrown to indicated something unrecoverable happened with transport. */
public final class TransportBrokenException extends TransportException {
  private static final long serialVersionUID = 1L;

  public TransportBrokenException(String message) {
    super(null, message);
  }

  public TransportBrokenException(ITransportChannel transport, String message) {
    super(transport, message);
  }
}
