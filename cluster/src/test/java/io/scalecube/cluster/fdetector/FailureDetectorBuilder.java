package io.scalecube.cluster.fdetector;

import io.scalecube.transport.Transport;
import io.scalecube.transport.TransportEndpoint;
import io.scalecube.transport.TransportSettings;

import com.google.common.base.Throwables;

import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.List;

public class FailureDetectorBuilder {
  final FailureDetector target;

  FailureDetectorBuilder(TransportEndpoint transportEndpoint, Transport tf) {
    target = new FailureDetector(transportEndpoint, Schedulers.from(tf.getWorkerGroup()));
    target.setTransport(tf);
  }

  public FailureDetectorBuilder set(List<TransportEndpoint> members) {
    target.setClusterEndpoints(members);
    return this;
  }

  public FailureDetectorBuilder pingTime(int pingTime) {
    target.setPingTime(pingTime);
    return this;
  }

  public FailureDetectorBuilder pingTimeout(int pingTimeout) {
    target.setPingTimeout(pingTimeout);
    return this;
  }

  public FailureDetectorBuilder ping(TransportEndpoint member) {
    target.setPingMember(member);
    return this;
  }

  public FailureDetectorBuilder noRandomMembers() {
    target.setRandomMembers(Arrays.asList(new TransportEndpoint[0]));
    return this;
  }

  public FailureDetectorBuilder randomMembers(List<TransportEndpoint> members) {
    target.setRandomMembers(members);
    return this;
  }

  public FailureDetectorBuilder block(TransportEndpoint dest) {
    Transport tf = (Transport) target.getTransport();
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
    Transport tf = (Transport) target.getTransport();
    tf.unblockAll();
    return this;
  }

  public static FailureDetectorBuilder FDBuilder(TransportEndpoint transportEndpoint) {
    TransportSettings transportSettings = TransportSettings.builder().useNetworkEmulator(true).build();
    Transport transport = Transport.newInstance(transportEndpoint, transportSettings);
    return new FailureDetectorBuilder(transportEndpoint, transport);
  }

  public static FailureDetectorBuilder FDBuilder(TransportEndpoint transportEndpoint, Transport tf) {
    return new FailureDetectorBuilder(transportEndpoint, tf);
  }

  public FailureDetector target() {
    return target;
  }

  public FailureDetectorBuilder init() {
    try {
      target.getTransport().start().get();
    } catch (Exception ex) {
      Throwables.propagate(ex);
    }
    target.start();
    return this;
  }
}
