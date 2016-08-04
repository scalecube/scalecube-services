package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportEndpoint;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Throwables;

import java.util.Arrays;
import java.util.List;

public class FailureDetectorBuilder {
  final FailureDetector failureDetector;

  FailureDetectorBuilder(Transport transport, FailureDetectorSettings settings) {
    failureDetector = new FailureDetector(transport, settings);
  }

  public FailureDetectorBuilder set(List<TransportEndpoint> members) {
    failureDetector.setClusterEndpoints(members);
    return this;
  }

  public FailureDetectorBuilder pingMember(TransportEndpoint member) {
    failureDetector.setPingMember(member);
    return this;
  }

  public FailureDetectorBuilder noRandomMembers() {
    failureDetector.setRandomMembers(Arrays.asList(new TransportEndpoint[0]));
    return this;
  }

  public FailureDetectorBuilder randomMembers(List<TransportEndpoint> members) {
    failureDetector.setRandomMembers(members);
    return this;
  }

  public FailureDetectorBuilder block(TransportEndpoint dest) {
    Transport tf = (Transport) failureDetector.getTransport();
    tf.blockMessagesTo(dest);
    return this;
  }

  public FailureDetectorBuilder block(List<TransportEndpoint> members) {
    for (TransportEndpoint dest : members) {
      block(dest);
    }
    return this;
  }

  public FailureDetectorBuilder unblockAll() {
    Transport tf = (Transport) failureDetector.getTransport();
    tf.unblockAll();
    return this;
  }

  public static FailureDetectorBuilder FDBuilder(TransportEndpoint transportEndpoint) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(transportEndpoint, transportSettings);
    return new FailureDetectorBuilder(transport, FailureDetectorSettings.DEFAULT);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTimeout(TransportEndpoint transportEndpoint, int pingTimeout) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(transportEndpoint, transportSettings);
    FailureDetectorSettings settings = FailureDetectorSettings.builder().pingTimeout(pingTimeout).build();
    return new FailureDetectorBuilder(transport, settings);
  }

  public static FailureDetectorBuilder FDBuilderWithPingTime(TransportEndpoint transportEndpoint, int pingTime) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(transportEndpoint, transportSettings);
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
