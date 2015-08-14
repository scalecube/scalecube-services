package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

import io.servicefabric.transport.protocol.Message;

@Immutable
public final class TransportMessage {

	private final Message message;
	private final ITransportChannel originChannel;
	private final TransportEndpoint originEndpoint;
	private final String originEndpointId;

	public TransportMessage(
			@CheckForNull ITransportChannel originChannel,
			@CheckForNull Message message,
			@CheckForNull TransportEndpoint originEndpoint,
			@CheckForNull String originEndpointId) {
		checkArgument(originChannel != null);
		checkArgument(message != null);
		checkArgument(originEndpoint != null);
		checkArgument(originEndpointId != null);
		this.originChannel = originChannel;
		this.message = message;
		this.originEndpoint = originEndpoint;
		this.originEndpointId = originEndpointId;
	}

	public Message message() {
		return message;
	}

	public ITransportChannel originChannel() {
		return originChannel;
	}

	public TransportEndpoint originEndpoint() {
		return originEndpoint;
	}

	public String originEndpointId() {
		return originEndpointId;
	}
}
