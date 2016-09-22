package io.scalecube.leaderelection;

import io.scalecube.cluster.Member;
import io.scalecube.transport.Address;

/**
 * Event which is emitted on cluster leadership changes when member state changed from FOLLOWER, CANDIDATE, LEADER.
 * 
 * @author Ronen Nachmias
 *
 */
public class LeadershipEvent {

  public enum Type {
    BECAME_LEADER, LEADERSHIP_REVOKED, NEW_LEADER
  }

  private final Type type;
  private final Member member;

  private LeadershipEvent(Type type, Member who) {
    this.type = Type.NEW_LEADER;
    this.member = who;
  }

  public Type type() {
    return type;
  }

  public Member member() {
    return this.member;
  }

  public static LeadershipEvent becameLeader(Member member) {
    return new LeadershipEvent(Type.BECAME_LEADER, member);
  }

  public static LeadershipEvent newLeader(Member member) {
    return new LeadershipEvent(Type.NEW_LEADER, member);
  }

  public static LeadershipEvent leadershipRevoked(Member member) {
    return new LeadershipEvent(Type.LEADERSHIP_REVOKED, member);
  }



}
