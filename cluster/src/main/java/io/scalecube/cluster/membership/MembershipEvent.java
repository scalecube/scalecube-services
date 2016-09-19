package io.scalecube.cluster.membership;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Member;

import javax.annotation.concurrent.Immutable;

/**
 * Event which is emitted on cluster membership changes when new member added or removed from cluster.
 *
 * @author Anton Kharenko
 */
@Immutable
public class MembershipEvent {

  public enum Type {
    ADDED, REMOVED
  }

  private final Type type;
  private final Member member;

  public MembershipEvent(Type type, Member member) {
    checkArgument(type != null);
    checkArgument(member != null);
    this.type = type;
    this.member = member;
  }

  public Type type() {
    return type;
  }

  public boolean isAdded() {
    return type == Type.ADDED;
  }

  public boolean isRemoved() {
    return type == Type.REMOVED;
  }

  public Member member() {
    return member;
  }

  @Override
  public String toString() {
    return "MembershipEvent{type=" + type + ", member=" + member + '}';
  }
}
