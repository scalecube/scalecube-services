package io.servicefabric.transport;

/** Thrown to indicate that some issue happened at handshake process between two transport endpoints. */
public final class TransportHandshakeException extends TransportException {
  private static final long serialVersionUID = 1L;

  private final TransportData handshake;

  public TransportHandshakeException(ITransportChannel transport, TransportData handshake) {
    super(transport);
    this.handshake = handshake;
  }

  public TransportHandshakeException(ITransportChannel transport, TransportData handshake, Throwable cause) {
    super(transport, cause);
    this.handshake = handshake;
  }

  public TransportData getHandshake() {
    return handshake;
  }
}
