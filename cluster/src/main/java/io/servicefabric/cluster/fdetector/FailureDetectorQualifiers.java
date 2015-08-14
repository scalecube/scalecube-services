package io.servicefabric.cluster.fdetector;

import rx.functions.Func1;

import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.TransportMessage;

public final class FailureDetectorQualifiers {
	public static final String PING = "pt.openapi.core.cluster/fdetector/ping";
	public static final String PING_REQ = "pt.openapi.core.cluster/fdetector/pingReq";
	public static final String ACK = "pt.openapi.core.cluster/fdetector/ack";

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

	private FailureDetectorQualifiers() {
	}

	static Func1<TransportMessage, Boolean> ackFilter() {
		return new QFilter(ACK);
	}

	static Func1<TransportMessage, Boolean> ackFilter(String correlationId) {
		return new QFilter(ACK, correlationId);
	}

	static Func1<TransportMessage, Boolean> pingFilter() {
		return new QFilter(PING);
	}

	static Func1<TransportMessage, Boolean> pingReqFilter() {
		return new QFilter(PING_REQ);
	}

	static Func1<TransportMessage, Boolean> targetFilter(final ClusterEndpoint endpoint) {
		return new Func1<TransportMessage, Boolean>() {
			@Override
			public Boolean call(TransportMessage transportMessage) {
				return ((FailureDetectorData) transportMessage.message().data()).getTo().equals(endpoint);
			}
		};
	}
}
