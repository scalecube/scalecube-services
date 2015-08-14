package io.servicefabric.transport;

/** Generic transport exception. */
public class TransportException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	protected final ITransportChannel transport;

	public TransportException(ITransportChannel transport) {
		this.transport = transport;
	}

	public TransportException(ITransportChannel transport, String message) {
		super(message);
		this.transport = transport;
	}

	public TransportException(ITransportChannel transport, String message, Throwable cause) {
		super(message, cause);
		this.transport = transport;
	}

	public TransportException(ITransportChannel transport, Throwable cause) {
		super(cause);
		this.transport = transport;
	}

	public final ITransportChannel getTransport() {
		return transport;
	}
}
