package io.scalecube.cluster.membership;

public enum MemberStatus {

  /**
   * Member is reachable and responding on messages.
   */
  ALIVE,

  /**
   * Member can't be reached and marked as suspected to be failed.
   */
  SUSPECT,

  /**
   * Member declared as dead after being {@link #SUSPECT} for configured time or when node has been gracefully shutdown
   * and left cluster.
   */
  DEAD

}
