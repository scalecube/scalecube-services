package io.servicefabric.cluster;

import rx.functions.Func1;

import io.servicefabric.cluster.gossip.Gossip;
import io.servicefabric.transport.TransportMessage;

public final class ClusterMembershipQualifiers {
	public static final String SYNC = "pt.openapi.core.cluster/membership/sync";
	public static final String SYNC_ACK = "pt.openapi.core.cluster/membership/syncAck";
	public static final String GOSSIP_MEMBERSHIP = "pt.openapi.core.cluster/membership/gossip";

	private static class QFilter implements Func1<TransportMessage, Boolean> {
		final String qualifier;
		final String correlationId;

		QFilter(String qualifier) {
			this(qualifier, null);
		}

		QFilter(String qualifier, String correlationId) {
			this.qualifier = qualifier;
			this.correlationId = correlationId;
		}

		@Override
		public Boolean call(TransportMessage transportMessage) {
			boolean q0 = qualifier.equals(transportMessage.message().qualifier());
			boolean q1 = correlationId == null || correlationId.equals(transportMessage.message().correlationId());
			return q0 && q1;
		}
	}

	private static class QFilterGossip implements Func1<Gossip, Boolean> {
		final String qualifier;

		QFilterGossip(String qualifier) {
			this.qualifier = qualifier;
		}

		@Override
		public Boolean call(Gossip gossip) {
			return qualifier.equalsIgnoreCase(gossip.getQualifier());
		}
	}

	private ClusterMembershipQualifiers() {
	}

	static QFilter syncFilter() {
		return new QFilter(SYNC);
	}

	static QFilter syncAckFilter(String correlationId) {
		return new QFilter(SYNC_ACK, correlationId);
	}

	static QFilterGossip gossipMembershipFilter() {
		return new QFilterGossip(GOSSIP_MEMBERSHIP);
	}
}
