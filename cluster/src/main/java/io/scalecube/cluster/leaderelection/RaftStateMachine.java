package io.scalecube.cluster.leaderelection;


import unquietcode.tools.esm.EnumStateMachine;

/**
 * Created by ronenn on 9/14/2016.
 */
public abstract class RaftStateMachine {

  private EnumStateMachine<StateType> stateMachine;

  public RaftStateMachine() {
    stateMachine = new EnumStateMachine<StateType>(StateType.START);

    stateMachine.addTransitions(StateType.START, StateType.FOLLOWER);
    stateMachine.addTransitions(StateType.FOLLOWER, StateType.CANDIDATE);
    stateMachine.addTransitions(StateType.CANDIDATE, StateType.LEADER);
    stateMachine.addTransitions(StateType.CANDIDATE, StateType.FOLLOWER);
    stateMachine.addTransitions(StateType.LEADER, StateType.FOLLOWER);

    stateMachine.addTransitions(StateType.START, StateType.STOPPED);
    stateMachine.addTransitions(StateType.FOLLOWER, StateType.STOPPED);
    stateMachine.addTransitions(StateType.CANDIDATE, StateType.STOPPED);
    stateMachine.addTransitions(StateType.LEADER, StateType.STOPPED);

    stateMachine.onEntering(StateType.FOLLOWER, state -> {
      becomeFollower();
    });

    stateMachine.onEntering(StateType.CANDIDATE, state -> {
      becomeCanidate();
    });

    stateMachine.onEntering(StateType.LEADER, state -> {
      becomeLeader();
    });
  }

  public StateType currentState() {
    return stateMachine.currentState();
  }

  public void transition(StateType state) {
    stateMachine.transition(state);
  }

  protected abstract void becomeFollower();

  protected abstract void becomeCanidate();

  protected abstract void becomeLeader();
}
