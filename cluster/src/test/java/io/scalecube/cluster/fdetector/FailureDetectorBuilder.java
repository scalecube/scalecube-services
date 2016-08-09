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

  public FailureDetectorBuilder set(List<Address> members) {
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

  public static FailureDetectorBuilder FDBuilder(Address localAddress) {
    TransportConfig transportConfig = TransportConfig.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportConfig);
    return new FailureDetectorBuilder(transport, FailureDetectorConfig.DEFAULT);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTimeout(Address localAddress, int pingTimeout) {
    TransportConfig transportConfig = TransportConfig.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportConfig);
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder().pingTimeout(pingTimeout).build();
    return new FailureDetectorBuilder(transport, failureDetectorConfig);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTime(Address localAddress, int pingTime) {
    TransportConfig transportConfig = TransportConfig.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportConfig);
    FailureDetectorConfig failureDetectorConfig = FailureDetectorConfig.builder().pingTime(pingTime).build();
    return new FailureDetectorBuilder(transport, failureDetectorConfig);
  }

  public FailureDetectorBuilder init() {
    try {
      failureDetector.getTransport().start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    failureDetector.start();
    return this;
  }
}
