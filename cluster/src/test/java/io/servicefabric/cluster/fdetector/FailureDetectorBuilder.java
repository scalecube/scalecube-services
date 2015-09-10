package io.servicefabric.cluster.fdetector;

import io.servicefabric.cluster.ClusterEndpoint;
import io.servicefabric.transport.SocketChannelPipelineFactory;
import io.servicefabric.transport.Transport;
import io.servicefabric.transport.TransportBuilder;
import io.servicefabric.transport.TransportEndpoint;

import rx.schedulers.Schedulers;

import java.util.Arrays;
import java.util.List;

public class FailureDetectorBuilder {
  final FailureDetector target;

  FailureDetectorBuilder(ClusterEndpoint clusterEndpoint, Transport tf) {
    target = new FailureDetector(clusterEndpoint, Schedulers.from(tf.getEventExecutor()));
    target.setTransport(tf);
  }

  public FailureDetectorBuilder set(List<ClusterEndpoint> members) {
    target.setClusterMembers(members);
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

  public FailureDetectorBuilder ping(ClusterEndpoint member) {
    target.setPingMember(member);
    return this;
  }

  public FailureDetectorBuilder noRandomMembers() {
    target.setRandomMembers(Arrays.asList(new ClusterEndpoint[0]));
    return this;
  }

  public FailureDetectorBuilder randomMembers(List<ClusterEndpoint> members) {
    target.setRandomMembers(members);
    return this;
  }

  public FailureDetectorBuilder block(ClusterEndpoint dest) {
    Transport tf = (Transport) target.getTransport();
    SocketChannelPipelineFactory pf = tf.getPipelineFactory();
    pf.blockMessagesTo(dest.endpoint());
    return this;
  }

  public FailureDetectorBuilder block(List<ClusterEndpoint> members) {
    for (ClusterEndpoint dest : members) {
      block(dest);
    }
    return this;
  }

  public FailureDetectorBuilder network(ClusterEndpoint member, int lostPercent, int mean) {
    Transport tf = (Transport) target.getTransport();
    SocketChannelPipelineFactory pf = tf.getPipelineFactory();
    pf.setNetworkSettings(member.endpoint(), lostPercent, mean);
    return this;
  }

  public FailureDetectorBuilder unblock(TransportEndpoint dest) {
    Transport tf = (Transport) target.getTransport();
    SocketChannelPipelineFactory pf = tf.getPipelineFactory();
    pf.unblockMessagesTo(dest);
    return this;
  }

  public FailureDetectorBuilder unblock(List<TransportEndpoint> members) {
    for (TransportEndpoint dest : members) {
      unblock(dest);
    }
    return this;
  }

  public FailureDetectorBuilder unblockAll() {
    Transport tf = (Transport) target.getTransport();
    SocketChannelPipelineFactory pf = tf.getPipelineFactory();
    pf.unblockAll();
    return this;
  }

  public static FailureDetectorBuilder FDBuilder(ClusterEndpoint clusterEndpoint) {
    Transport transport =
        (Transport) TransportBuilder.newInstance(clusterEndpoint.endpoint(), clusterEndpoint.endpointId())
            .useNetworkEmulator().build();
    return new FailureDetectorBuilder(clusterEndpoint, transport);
  }

  public static FailureDetectorBuilder FDBuilder(ClusterEndpoint clusterEndpoint, Transport tf) {
    return new FailureDetectorBuilder(clusterEndpoint, tf);
  }

  public FailureDetector target() {
    return target;
  }

  public FailureDetectorBuilder init() {
    target.getTransport().start();
    target.start();
    return this;
  }
}
