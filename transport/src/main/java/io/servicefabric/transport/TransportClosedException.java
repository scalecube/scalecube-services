package io.servicefabric.transport;

/** Thrown to indicate that transport is already closed and operations with this transport can't progress anymore. */
public final class TransportClosedException extends TransportException {
	private static final long serialVersionUID = 1L;

	public TransportClosedException(ITransportChannel transport) {
		super(transport);
	}

	public TransportClosedException(ITransportChannel transport, Throwable cause) {
		super(transport, cause);
	}
}
