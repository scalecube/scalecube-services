package io.servicefabric.cluster.fdetector;

import io.servicefabric.cluster.ClusterEndpoint;
import io.protostuff.Tag;

/** DTO class. Supports FailureDetector messages (Ping, Ack, PingReq). */
public final class FailureDetectorData {
	/** Message's source endpoint */
	@Tag(1)
	private ClusterEndpoint from;
	/** Message's destination endpoint */
	@Tag(2)
	private ClusterEndpoint to;
	/** Endpoint, who originally initiated ping sequence */
	@Tag(3)
	private ClusterEndpoint originalIssuer;

	public FailureDetectorData(ClusterEndpoint from, ClusterEndpoint to) {
		this.from = from;
		this.to = to;
	}

	public FailureDetectorData(ClusterEndpoint from, ClusterEndpoint to, ClusterEndpoint originalIssuer) {
		this.from = from;
		this.to = to;
		this.originalIssuer = originalIssuer;
	}

	public ClusterEndpoint getFrom() {
		return from;
	}

	public ClusterEndpoint getTo() {
		return to;
	}

	public ClusterEndpoint getOriginalIssuer() {
		return originalIssuer;
	}

	@Override
	public String toString() {
		return "FailureDetectorData{" +
				", from=" + from +
				", to=" + to +
				", originalIssuer=" + originalIssuer +
				'}';
	}
}
