package io.servicefabric.transport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagate;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

import com.google.common.base.Splitter;
import io.servicefabric.transport.utils.IpAddressResolver;

public final class TransportEndpoint {
	private String scheme;
	private String hostAddress;
	private int port = -1;

	private TransportEndpoint() {
	}

	/**
	 * @param basePort base port
	 * @param portShift port shift
	 * @return new local (in terms of tcp) TransportEndpoint object; resolves real ip. 
	 * @throws IllegalArgumentException if {@code basePort} is invalid / or {@code portShift} is invalid
	 */
	public static TransportEndpoint localTcp(int basePort, int portShift) throws IllegalArgumentException {
		checkArgument(basePort > 0);
		checkArgument(portShift > 0);
		TransportEndpoint endpoint = new TransportEndpoint();
		endpoint.scheme = "tcp";
		endpoint.hostAddress = resolveIp();
		endpoint.port = basePort + portShift;
		return endpoint;
	}

	/**
	 * @param uri must come in form {@code {tcp|local}://[host:]port}
	 * @return TransportEndpoint object
	 * @throws IllegalArgumentException if scheme is wrong / or port is invalid
	 */
	public static TransportEndpoint from(String uri) throws IllegalArgumentException {
		TransportEndpoint target = new TransportEndpoint();
		URI uri1 = URI.create(uri);
		target.scheme = uri1.getScheme();
		switch (target.scheme) {
		case "tcp":
			int port = uri1.getPort();
			if (port == -1) {
				try {
					target.port = Integer.valueOf(uri1.getAuthority());
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("NFE invalid port, uri=" + uri);
				}
				target.hostAddress = resolveIp();
			} else {
				target.port = port;
				String host = uri1.getHost();
				target.hostAddress = isLocal(host) ? resolveIp() : uri1.getHost();
			}
			break;
		case "local":
			target.hostAddress = uri1.getAuthority() + uri1.getPath();
			break;
		default:
			throw new IllegalArgumentException(uri);
		}
		return target;
	}

	public static TransportEndpoint tcp(String hostnameAndPort) {
		List<String> addressAndPort = Splitter.on(':').splitToList(hostnameAndPort);
		if (addressAndPort.size() != 2) {
			throw new IllegalArgumentException("hostname:port expected, token=" + hostnameAndPort);
		}
		TransportEndpoint endpoint = new TransportEndpoint();
		try {
			endpoint.port = Integer.valueOf(addressAndPort.get(1).trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("NFE invalid port, token=" + hostnameAndPort);
		}
		String address = addressAndPort.get(0).trim();
		endpoint.hostAddress = isLocal(address) ? resolveIp() : address;
		endpoint.scheme = "tcp";
		return endpoint;
	}

	public String getScheme() {
		return scheme;
	}

	public String getHostAddress() {
		return hostAddress;
	}

	public int getPort() {
		return port;
	}

	private static boolean isLocal(String host) {
		return "localhost".equals(host) || "127.0.0.1".equals(host);
	}

	private static String resolveIp() {
		try {
			return IpAddressResolver.resolveIpAddress().getHostAddress();
		} catch (UnknownHostException e) {
			throw propagate(e);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		TransportEndpoint endpoint = (TransportEndpoint) o;

		if (port != endpoint.port)
			return false;
		if (hostAddress != null ? !hostAddress.equals(endpoint.hostAddress) : endpoint.hostAddress != null)
			return false;
		if (scheme != null ? !scheme.equals(endpoint.scheme) : endpoint.scheme != null)
			return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = scheme != null ? scheme.hashCode() : 0;
		result = 31 * result + (hostAddress != null ? hostAddress.hashCode() : 0);
		result = 31 * result + port;
		return result;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder(scheme);
		sb.append("://");
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
