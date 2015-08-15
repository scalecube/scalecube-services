package io.servicefabric.cluster;

import static com.google.common.base.Preconditions.checkArgument;
import io.servicefabric.transport.TransportEndpoint;

import java.net.URI;

import javax.annotation.CheckForNull;
import javax.annotation.concurrent.Immutable;

@Immutable
public final class ClusterEndpoint {
	private TransportEndpoint endpoint;
	private String endpointId;

	private ClusterEndpoint() {
	}

	public ClusterEndpoint(@CheckForNull TransportEndpoint endpoint, @CheckForNull String endpointId) {
		checkArgument(endpoint != null);
		checkArgument(endpointId != null);
		this.endpoint = endpoint;
		this.endpointId = endpointId;
	}

	public static ClusterEndpoint from(String uri) {
		URI uri1 = URI.create(uri);
		ClusterEndpoint target = new ClusterEndpoint();
		target.endpointId = uri1.getUserInfo();
		String substring = uri1.getSchemeSpecificPart().substring(uri1.getSchemeSpecificPart().indexOf("@") + 1);
		target.endpoint = TransportEndpoint.from(uri1.getScheme() + "://" + substring);
		return target;
	}

	public static ClusterEndpoint from(String endpointId, TransportEndpoint transportEndpoint) {
		return new ClusterEndpoint(transportEndpoint, endpointId);
	}

	public TransportEndpoint endpoint() {
		return endpoint;
	}

	public String endpointId() {
		return endpointId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		ClusterEndpoint that = (ClusterEndpoint) o;

		if (!endpointId.equals(that.endpointId))
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		return endpointId.hashCode();
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(endpoint.getScheme());
		sb.append("://").append(endpointId);
		sb.append("@");
		String hostAddress = endpoint.getHostAddress();
		int port = endpoint.getPort();
		if (hostAddress != null) {
			sb.append(hostAddress);
		}
		if (port > 0) {
			if (hostAddress != null) {
				sb.append(":");
			}
			sb.append(port);
		}
		return sb.toString();
	}
}
