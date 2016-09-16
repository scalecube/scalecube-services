package io.scalecube.leaderelection;

import io.scalecube.cluster.ICluster;
import io.scalecube.transport.Address;

/**
 * Created by ronenn on 9/12/2016.
 */
public interface LeaderElection {

	Address leader();

	void addStateListener(IStateListener handler);

	RaftLeaderElection start();

	ICluster cluster();
}
