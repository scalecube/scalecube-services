package io.servicefabric.cluster.fdetector.api;

import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.cluster.fdetector.FailureDetectorEvent;

import java.util.Collection;

import rx.Observable;

public interface IFailureDetector {

	void start();

	void stop();

	/** Listens for SUSPECTED/TRUSTED members. */
	Observable<FailureDetectorEvent> listenStatus();

	/** Marks given member as SUSPECTED inside FD algorithm internals. */
	void suspect(ClusterEndpoint member);

	/** Marks given member as TRUSTED inside FD algorithm internals. */
	void trust(ClusterEndpoint member);

	/** Updates list of cluster members among which should work FD algorithm. */
	void setClusterMembers(Collection<ClusterEndpoint> members);
}
