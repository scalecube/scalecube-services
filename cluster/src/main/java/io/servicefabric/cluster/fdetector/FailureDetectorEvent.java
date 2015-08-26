package io.servicefabric.cluster.fdetector;

import static io.servicefabric.cluster.ClusterMemberStatus.SUSPECTED;
import static io.servicefabric.cluster.ClusterMemberStatus.TRUSTED;
import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.cluster.ClusterMemberStatus;

import com.google.common.base.Objects;

/** Tuple class. Contains cluster endpoint and its status. */
public final class FailureDetectorEvent {
	private final ClusterEndpoint endpoint;
	private final ClusterMemberStatus status;

	private FailureDetectorEvent(ClusterEndpoint endpoint, ClusterMemberStatus status) {
		this.endpoint = endpoint;
		this.status = status;
	}

	public static FailureDetectorEvent TRUSTED(ClusterEndpoint endpoint) {
		return new FailureDetectorEvent(endpoint, TRUSTED);
	}

	public static FailureDetectorEvent SUSPECTED(ClusterEndpoint endpoint) {
		return new FailureDetectorEvent(endpoint, SUSPECTED);
	}

	public ClusterEndpoint endpoint() {
		return endpoint;
	}

	public ClusterMemberStatus status() {
		return status;
	}

	@Override
	public String toString() {
		return "FailureDetectorEvent{" +
				"endpoint=" + endpoint +
				", status=" + status +
				'}';
	}
}
