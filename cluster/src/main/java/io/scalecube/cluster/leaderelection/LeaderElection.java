package io.scalecube.cluster.leaderelection;

import io.scalecube.transport.Address;

/**
 * Created by ronenn on 9/12/2016.
 */
public interface LeaderElection {

    Address leader();

    void addStateListener(IStateListener handler);

    void start();
}
