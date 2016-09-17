package io.scalecube.services.leaderelection;

/**
 * Created by ronenn on 9/12/2016.
 */
public interface IStateListener {
    public void onState(RaftLeaderElection.State state);
}
