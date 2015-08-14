package io.servicefabric.transport;

/** Thrown to indicate that {@link ITransport} can't return transport client is asking for. */
public final class TransportNotFoundException extends TransportException {
	private static final long serialVersionUID = 1L;

	public TransportNotFoundException(String message) {
		super(null, message);
	}

	public TransportNotFoundException(String message, Throwable cause) {
		super(null, message, cause);
	}

	public TransportNotFoundException(Throwable cause) {
		super(null, cause);
	}
}
