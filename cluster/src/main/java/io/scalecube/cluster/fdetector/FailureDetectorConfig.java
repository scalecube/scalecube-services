package io.scalecube.cluster.fdetector;

public interface FailureDetectorConfig {

  int getPingInterval();

  int getPingTimeout();

  int getPingReqMembers();

}
