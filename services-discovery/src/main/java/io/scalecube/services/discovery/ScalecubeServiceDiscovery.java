package io.scalecube.services.discovery;

import static io.scalecube.reactor.RetryNonSerializedEmitFailureHandler.RETRY_NON_SERIALIZED;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointAdded;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointLeaving;
import static io.scalecube.services.discovery.api.ServiceDiscoveryEvent.newEndpointRemoved;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.fdetector.FailureDetectorConfig;
import io.scalecube.cluster.gossip.GossipConfig;
import io.scalecube.cluster.membership.MembershipConfig;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.TransportConfig;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.nio.ByteBuffer;
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
    this.clusterConfig = ClusterConfig.defaultLanConfig();
  }

  private ScalecubeServiceDiscovery(ScalecubeServiceDiscovery other) {
    this.clusterConfig = other.clusterConfig;
    this.cluster = other.cluster;
  }

  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig> op) {
    ScalecubeServiceDiscovery d = new ScalecubeServiceDiscovery(this);
    d.clusterConfig = op.apply(clusterConfig);
    return d;
  }

  public ScalecubeServiceDiscovery transport(UnaryOperator<TransportConfig> op) {
    return options(cfg -> cfg.transport(op));
  }

  public ScalecubeServiceDiscovery membership(UnaryOperator<MembershipConfig> op) {
    return options(cfg -> cfg.membership(op));
  }

  public ScalecubeServiceDiscovery gossip(UnaryOperator<GossipConfig> op) {
    return options(cfg -> cfg.gossip(op));
  }

  public ScalecubeServiceDiscovery failureDetector(UnaryOperator<FailureDetectorConfig> op) {
    return options(cfg -> cfg.failureDetector(op));
  }

  @Override
  public void start() {
    cluster =
        new ClusterImpl()
            .config(options -> clusterConfig)
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
    return cluster != null ? cluster.addresses().get(0) : null;
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listen() {
    return sink.asFlux().onBackpressureBuffer();
  }

  @Override
  public void shutdown() {
    sink.emitComplete(RETRY_NON_SERIALIZED);
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
    sink.emitNext(discoveryEvent, RETRY_NON_SERIALIZED);
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
