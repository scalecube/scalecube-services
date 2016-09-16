package io.scalecube.leaderelection;

/**
 * Created by ronenn on 9/12/2016.
 */
public interface IStateListener {
  void onState(StateType state);
}
