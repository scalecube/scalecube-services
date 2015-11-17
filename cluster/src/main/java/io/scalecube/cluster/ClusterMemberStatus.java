package io.scalecube.cluster;

public enum ClusterMemberStatus {
  /** Member passed direct or indirect heartbeat. */
  TRUSTED,
  /** Member can't pass direct or indirect heartbeat. */
  SUSPECTED,
  /**
   * Member removed from {@link IClusterMembership} object after being {@link #SUSPECTED} for some time. <b>Not exposed
   * cluster wide.</b>
   * */
  REMOVED,
  /** Member has been gracefully shutdown. */
  SHUTDOWN
}
