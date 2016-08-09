package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportConfig;

import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.List;

public class FailureDetectorBuilder {
  final FailureDetector failureDetector;

  FailureDetectorBuilder(Transport transport, FailureDetectorConfig failureDetectorConfig) {
    failureDetector = new FailureDetector(transport, failureDetectorConfig);
  }

  public FailureDetectorBuilder members(List<Address> members) {
    failureDetector.setMembers(members);
    return this;
  }

  public FailureDetectorBuilder pingMember(Address member) {
    failureDetector.setPingMember(member);
    return this;
  }

  public FailureDetectorBuilder noRandomMembers() {
    failureDetector.setRandomMembers(Arrays.asList(new Address[0]));
    return this;
  }

  public FailureDetectorBuilder randomMembers(List<Address> members) {
    failureDetector.setRandomMembers(members);
    return this;
  }

  public FailureDetectorBuilder block(Address dest) {
    Transport tf = (Transport) failureDetector.getTransport();
    tf.blockMessagesTo(dest);
    return this;
  }

  public FailureDetectorBuilder block(List<Address> members) {
    for (Address dest : members) {
      block(dest);
    }
    return this;
  }

  public FailureDetectorBuilder unblockAll() {
    Transport tf = (Transport) failureDetector.getTransport();
    tf.unblockAll();
    return this;
  }

  public static FailureDetectorBuilder FDBuilder(Transport transport) {
    return new FailureDetectorBuilder(transport, FailureDetectorConfig.DEFAULT);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTimeout(Transport transport, int pingTimeout) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder().pingTimeout(pingTimeout).build();
    return new FailureDetectorBuilder(transport, failureDetectorConfig);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTime(Transport transport, int pingTime) {
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder().pingTime(pingTime).build();
    return new FailureDetectorBuilder(transport, failureDetectorConfig);
  }

  public FailureDetectorBuilder start() {
    failureDetector.start();
    return this;
  }
}
