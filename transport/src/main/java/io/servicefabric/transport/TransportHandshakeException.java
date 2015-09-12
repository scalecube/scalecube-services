package io.servicefabric.transport;

/** Thrown to indicate that some issue happened at handshake process between two transport endpoints. */
public final class TransportHandshakeException extends TransportException {
  private static final long serialVersionUID = 1L;

  private final TransportHandshakeData handshake;

  public TransportHandshakeException(ITransportChannel transport, TransportHandshakeData handshake) {
    super(transport);
    this.handshake = handshake;
  }

  public TransportHandshakeException(ITransportChannel transport, TransportHandshakeData handshake, Throwable cause) {
    super(transport, cause);
    this.handshake = handshake;
  }

  public TransportHandshakeData getHandshake() {
    return handshake;
  }
}
