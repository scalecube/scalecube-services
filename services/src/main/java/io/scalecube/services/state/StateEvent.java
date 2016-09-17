package io.scalecube.services.state;

import javax.annotation.concurrent.Immutable;

import rx.functions.Func1;

/**
 * State event emitted by {@code DistributedState} in case of state was changed on one of the cluster members.
 *
 * @param <S> State object class. This object should be immutable otherwise state maintenance algorithm won't work
 *        correctly.
 * @author Anton Kharenko
 * @see DistributedState
 */
@Immutable
public final class StateEvent<S> {

  public static final Func1<StateEvent, Boolean> ADDED_FILTER = new StateEvent.EventTypeFilter(Type.ADDED);
  public static final Func1<StateEvent, Boolean> UPDATED_FILTER = new StateEvent.EventTypeFilter(Type.UPDATED);
  public static final Func1<StateEvent, Boolean> REMOVED_FILTER = new StateEvent.EventTypeFilter(Type.REMOVED);

  public enum Type {
    ADDED,
    UPDATED,
    REMOVED
  }

  private final Type type;
  private final String memberId;
  private final S oldState;
  private final S newState;

  private StateEvent(Type type, String memberId, S oldState, S newState) {
    this.type = type;
    this.memberId = memberId;
    this.oldState = oldState;
    this.newState = newState;
  }

  static <S> StateEvent<S> createAddedEvent(String memberId, S addedState) {
    return new StateEvent<>(Type.ADDED, memberId, null, addedState);
  }

  static <S> StateEvent<S> createRemovedEvent(String memberId, S removedState) {
    return new StateEvent<>(Type.REMOVED, memberId, removedState, null);
  }

  static <S> StateEvent<S> createUpdatedEvent(String memberId, S oldState, S newState) {
    return new StateEvent<>(Type.UPDATED, memberId, oldState, newState);
  }

  /** Returns type of state event. */
  public Type type() {
    return type;
  }

  /** Returns member id on which state was changed. */
  public String memberId() {
    return memberId;
  }

  /** Returns previously known local state. In case of ADDED event it will be null. */
  public S oldState() {
    return oldState;
  }

  /** Returns new state. In case of REMOVED event it will be null.  */
  public S newState() {
    return newState;
  }

  @Override
  public String toString() {
    return "StateEvent{" +
        "type=" + type +
        ", memberId='" + memberId + '\'' +
        ", oldState=" + oldState +
        ", newState=" + newState +
        '}';
  }

  private static class EventTypeFilter implements Func1<StateEvent, Boolean> {
    private final Type type;

    public EventTypeFilter(Type type) {
      this.type = type;
    }

    @Override
    public Boolean call(StateEvent event) {
      return this.type == event.type;
    }
  }
}
