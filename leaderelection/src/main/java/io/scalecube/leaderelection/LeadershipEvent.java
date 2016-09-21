package io.scalecube.leaderelection;

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
  private final Address address;

  private LeadershipEvent(Type type, Address who) {
    this.type = Type.NEW_LEADER;
    this.address = who;
  }

  public Type type() {
    return type;
  }
  
  public Address address() {
    return this.address ;
  }
  
  public static LeadershipEvent becameLeader(Address address) { 
    return new LeadershipEvent(Type.BECAME_LEADER,address);
  }
  
  public static LeadershipEvent newLeader(Address address) {
    return new LeadershipEvent(Type.NEW_LEADER,address);
  }
  
  public static LeadershipEvent leadershipRevoked(Address address) {
    return new LeadershipEvent(Type.LEADERSHIP_REVOKED,address);
  }

  

}
