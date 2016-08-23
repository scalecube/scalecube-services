package io.scalecube.cluster;

public enum ClusterMemberStatus {
  /**
   * Member is reachable and responding on pings.
   */
  TRUSTED,

  /**
   * Member can't be reached and marked as suspected to be failed.
   */
  SUSPECTED,

  /**
   * Member removed from membership table after being {@link #SUSPECTED} for configured time.
   */
  REMOVED,

  /**
   * Member has been gracefully shutdown and left cluster.
   */
  SHUTDOWN
}
