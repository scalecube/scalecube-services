package io.scalecube.services.discovery;

import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointAdded;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointLeaving;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointRemoved;
import static reactor.core.publisher.Sinks.EmitFailureHandler.busyLooping;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.services.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public final class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private ClusterConfig clusterConfig;
  private Cluster cluster;

  // Sink
  private final Sinks.Many<ServiceDiscoveryEvent> sink =
      Sinks.many().multicast().directBestEffort();

  public ScalecubeServiceDiscovery() {
    this(new ClusterConfig());
  }

  public ScalecubeServiceDiscovery(Properties properties) {
    this(new ClusterConfig(properties));
  }

  public ScalecubeServiceDiscovery(ClusterConfig clusterConfig) {
    this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig");
  }

  public ClusterConfig clusterConfig() {
    return clusterConfig;
  }

  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig> op) {
    clusterConfig = Objects.requireNonNull(op.apply(clusterConfig), "clusterConfig");
    return this;
  }

  public ScalecubeServiceDiscovery transport(UnaryOperator<TransportConfig> op) {
    clusterConfig.transport(op);
    return this;
  }

  public ScalecubeServiceDiscovery membership(UnaryOperator<MembershipConfig> op) {
    clusterConfig.membership(op);
    return this;
  }

  public ScalecubeServiceDiscovery gossip(UnaryOperator<GossipConfig> op) {
    clusterConfig.gossip(op);
    return this;
  }

  public ScalecubeServiceDiscovery failureDetector(UnaryOperator<FailureDetectorConfig> op) {
    clusterConfig.failureDetector(op);
    return this;
  }

  @Override
  public void start() {
    cluster =
        new ClusterImpl(clusterConfig)
            .handler(
                cluster -> {
                  //noinspection CodeBlock2Expr
                  return new ClusterMessageHandler() {
                    @Override
                    public void onMembershipEvent(MembershipEvent event) {
                      ScalecubeServiceDiscovery.this.onMembershipEvent(event);
                    }
                  };
                })
            .startAwait();
  }

  @Override
  public Address address() {
    return cluster != null ? Address.from(cluster.address()) : null;
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  @Override
  public void shutdown() {
    sink.emitComplete(busyLooping(Duration.ofSeconds(3)));
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private void onMembershipEvent(MembershipEvent membershipEvent) {
    LOGGER.debug("onMembershipEvent: {}", membershipEvent);

    ServiceDiscoveryEvent discoveryEvent = toServiceDiscoveryEvent(membershipEvent);
    if (discoveryEvent == null) {
      LOGGER.warn(
          "DiscoveryEvent is null, cannot publish it (corresponding membershipEvent: {})",
          membershipEvent);
      return;
    }

    LOGGER.debug("Publish discoveryEvent: {}", discoveryEvent);
    sink.emitNext(discoveryEvent, busyLooping(Duration.ofSeconds(3)));
  }

  private ServiceDiscoveryEvent toServiceDiscoveryEvent(MembershipEvent membershipEvent) {
    ServiceDiscoveryEvent discoveryEvent = null;

    if (membershipEvent.isAdded() && membershipEvent.newMetadata() != null) {
      discoveryEvent = newEndpointAdded(toServiceEndpoint(membershipEvent.newMetadata()));
    }
    if (membershipEvent.isRemoved() && membershipEvent.oldMetadata() != null) {
      discoveryEvent = newEndpointRemoved(toServiceEndpoint(membershipEvent.oldMetadata()));
    }
    if (membershipEvent.isLeaving() && membershipEvent.newMetadata() != null) {
      discoveryEvent = newEndpointLeaving(toServiceEndpoint(membershipEvent.newMetadata()));
    }

    return discoveryEvent;
  }

  private ServiceEndpoint toServiceEndpoint(ByteBuffer byteBuffer) {
    try {
      return (ServiceEndpoint) clusterConfig.metadataCodec().deserialize(byteBuffer.duplicate());
    } catch (Exception e) {
      LOGGER.error("Failed to read metadata", e);
      throw Exceptions.propagate(e);
    }
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ScalecubeServiceDiscovery.class.getSimpleName() + "[", "]")
        .add("cluster=" + cluster)
        .add("clusterConfig=" + clusterConfig)
        .toString();
  }
}
