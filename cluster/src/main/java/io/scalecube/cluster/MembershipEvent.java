package io.scalecube.cluster;

import javax.annotation.concurrent.Immutable;

/**
 * @author Anton Kharenko
 */
@Immutable
public class MembershipEvent {

  public enum Type {ADDED, REMOVED}

  private final Type type;
  private final Member member;

  public MembershipEvent(Type type, Member member) {
    this.type = type;
    this.member = member;
  }

  public Type type() {
    return type;
  }

  public Member member() {
    return member;
  }

  @Override
  public String toString() {
    return "MembershipEvent{type=" + type + ", member=" + member + '}';
  }
}
