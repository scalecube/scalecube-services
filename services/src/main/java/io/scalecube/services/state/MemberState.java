package io.scalecube.services.state;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Objects;

import javax.annotation.concurrent.Immutable;

/**
 * Class which is a holder of state for the specific cluster member. It is used to provide ordering and synchronization
 * for state updates.
 * 
 * @param <S> State object class. This object should be immutable otherwise state maintenance algorithm won't work
 *        correctly.
 * @author Anton Kharenko
 * @see DistributedState
 */
@Immutable
public final class MemberState<S> {

  private final String stateId;
  private final String memberId;
  private final S state;
  private final long counter;

  private MemberState(String stateId, String memberId, S state, long counter) {
    checkArgument(stateId != null, "State id can't be null");
    checkArgument(memberId != null, "Member id can't be null");
    checkArgument(state != null, "State can't be null");
    this.stateId = stateId;
    this.memberId = memberId;
    this.state = state;
    this.counter = counter;
  }

  static <S> MemberState<S> newInstance(String stateId, String memberId, S state) {
    return newInstance(stateId, memberId, state, 0);
  }

  static <S> MemberState<S> newInstance(String stateId, String memberId, S state, long counter) {
    return new MemberState<>(stateId, memberId, state, counter);
  }

  MemberState<S> nextState(S newState) {
    return new MemberState<>(stateId, memberId, newState, counter + 1L);
  }

  public String stateId() {
    return stateId;
  }

  public String memberId() {
    return memberId;
  }

  public long counter() {
    return counter;
  }

  public S state() {
    return state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    MemberState<?> that = (MemberState<?>) o;
    return Objects.equals(counter, that.counter) && Objects.equals(memberId, that.memberId)
        && Objects.equals(stateId, that.stateId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(stateId, memberId, counter);
  }

  @Override
  public String toString() {
    return "MemberState{" + "stateId='" + stateId + '\'' + ", memberId='" + memberId + '\'' + ", state=" + state
        + ", counter=" + counter + '}';
  }
}
