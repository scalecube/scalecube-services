package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Transport;
import io.scalecube.transport.Address;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.List;

public class FailureDetectorBuilder {
  final FailureDetector failureDetector;

  FailureDetectorBuilder(Transport transport, FailureDetectorSettings settings) {
    failureDetector = new FailureDetector(transport, settings);
  }

  public FailureDetectorBuilder set(List<Address> members) {
    failureDetector.setClusterMembers(members);
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
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportSettings);
    return new FailureDetectorBuilder(transport, FailureDetectorSettings.DEFAULT);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTimeout(Address localAddress, int pingTimeout) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportSettings);
    FailureDetectorSettings settings = FailureDetectorSettings.builder().pingTimeout(pingTimeout).build();
    return new FailureDetectorBuilder(transport, settings);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTime(Address localAddress, int pingTime) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(localAddress, transportSettings);
    FailureDetectorSettings settings = FailureDetectorSettings.builder().pingTime(pingTime).build();
    return new FailureDetectorBuilder(transport, settings);
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
