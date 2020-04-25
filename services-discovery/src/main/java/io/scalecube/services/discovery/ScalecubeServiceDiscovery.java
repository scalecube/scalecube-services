package io.scalecube.services.discovery;

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
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Exceptions;
import reactor.core.publisher.DirectProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

public final class ScalecubeServiceDiscovery implements ServiceDiscovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscovery.class);

  private ServiceEndpoint serviceEndpoint;
  private ClusterConfig clusterConfig;
  private Cluster cluster;

  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.sink();

  /**
   * Constructor.
   *
   * @param serviceEndpoint service endpoint
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;
    this.clusterConfig = ClusterConfig.defaultLanConfig().metadata(serviceEndpoint);
  }

  /**
   * Copy constructor.
   *
   * @param other other instance
   */
  private ScalecubeServiceDiscovery(ScalecubeServiceDiscovery other) {
    this.serviceEndpoint = other.serviceEndpoint;
    this.clusterConfig = other.clusterConfig;
    this.cluster = other.cluster;
  }

  /**
   * Setter for {@code ClusterConfig} options.
   *
   * @param opts options operator
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig> opts) {
    ScalecubeServiceDiscovery d = new ScalecubeServiceDiscovery(this);
    d.clusterConfig = opts.apply(clusterConfig);
    return d;
  }

  /**
   * Setter for {@code TransportConfig} options.
   *
   * @param opts options operator
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery transport(UnaryOperator<TransportConfig> opts) {
    return options(cfg -> cfg.transport(opts));
  }

  /**
   * Setter for {@code MembershipConfig} options.
   *
   * @param opts options operator
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery membership(UnaryOperator<MembershipConfig> opts) {
    return options(cfg -> cfg.membership(opts));
  }

  /**
   * Setter for {@code GossipConfig} options.
   *
   * @param opts options operator
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery gossip(UnaryOperator<GossipConfig> opts) {
    return options(cfg -> cfg.gossip(opts));
  }

  /**
   * Setter for {@code FailureDetectorConfig} options.
   *
   * @param opts options operator
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery failureDetector(UnaryOperator<FailureDetectorConfig> opts) {
    return options(cfg -> cfg.failureDetector(opts));
  }

  @Override
  public Address address() {
    return cluster.address();
  }

  @Override
  public ServiceEndpoint serviceEndpoint() {
    return serviceEndpoint;
  }

  /**
   * Starts scalecube service discovery. Joins a cluster with local services as metadata.
   *
   * @return mono result
   */
  @Override
  public Mono<ServiceDiscovery> start() {
    return Mono.defer(
        () -> {
          // Start scalecube-cluster and listen membership events
          return new ClusterImpl()
              .config(options -> clusterConfig)
              .handler(
                  cluster -> {
                    return new ClusterMessageHandler() {
                      @Override
                      public void onMembershipEvent(MembershipEvent event) {
                        ScalecubeServiceDiscovery.this.onMembershipEvent(event);
                      }
                    };
                  })
              .start()
              .doOnSuccess(cluster -> this.cluster = cluster)
              .then(Mono.fromCallable(() -> JmxMonitorMBean.start(this)))
              .thenReturn(this);
        });
  }

  @Override
  public Flux<ServiceDiscoveryEvent> listenDiscovery() {
    return subject.onBackpressureBuffer();
  }

  @Override
  public Mono<Void> shutdown() {
    return Mono.defer(
        () -> {
          if (cluster == null) {
            sink.complete();
            return Mono.empty();
          }
          cluster.shutdown();
          return cluster.onShutdown().doFinally(s -> sink.complete());
        });
  }

  private void onMembershipEvent(MembershipEvent membershipEvent) {
    LOGGER.debug("onMembershipEvent: {}", membershipEvent);

    ServiceDiscoveryEvent discoveryEvent = toServiceDiscoveryEvent(membershipEvent);
    if (discoveryEvent == null) {
      LOGGER.warn(
          "Not publishing discoveryEvent, discoveryEvent is null, membershipEvent: {}",
          membershipEvent);
      return;
    }

    if (discoveryEvent != null) {
      LOGGER.debug("Publish discoveryEvent: {}", discoveryEvent);
      sink.next(discoveryEvent);
    }
  }

  private ServiceDiscoveryEvent toServiceDiscoveryEvent(MembershipEvent membershipEvent) {
    ServiceDiscoveryEvent discoveryEvent = null;

    if (membershipEvent.isAdded() && membershipEvent.newMetadata() != null) {
      discoveryEvent =
          ServiceDiscoveryEvent.newEndpointAdded(decodeMetadata(membershipEvent.newMetadata()));
    }

    if (membershipEvent.isRemoved() && membershipEvent.oldMetadata() != null) {
      discoveryEvent =
          ServiceDiscoveryEvent.newEndpointRemoved(decodeMetadata(membershipEvent.oldMetadata()));
    }

    if (membershipEvent.isLeaving() && membershipEvent.newMetadata() != null) {
      discoveryEvent =
          ServiceDiscoveryEvent.newEndpointLeaving(decodeMetadata(membershipEvent.newMetadata()));
    }

    return discoveryEvent;
  }

  private ServiceEndpoint decodeMetadata(ByteBuffer byteBuffer) {
    try {
      return (ServiceEndpoint) clusterConfig.metadataCodec().deserialize(byteBuffer.duplicate());
    } catch (Exception e) {
      LOGGER.error("Failed to read metadata: " + e);
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

  @SuppressWarnings("unused")
  public interface MonitorMBean {

    String getClusterConfig();

    String getDiscoveryAddress();

    String getRecentDiscoveryEvents();
  }

  private static class JmxMonitorMBean implements MonitorMBean {

    public static final int RECENT_DISCOVERY_EVENTS_SIZE = 128;

    private final ScalecubeServiceDiscovery discovery;
    private final List<ServiceDiscoveryEvent> recentDiscoveryEvents = new CopyOnWriteArrayList<>();

    private JmxMonitorMBean(ScalecubeServiceDiscovery discovery) {
      this.discovery = discovery;
    }

    private static JmxMonitorMBean start(ScalecubeServiceDiscovery instance) throws Exception {
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      JmxMonitorMBean jmxMBean = new JmxMonitorMBean(instance);
      jmxMBean.init();
      String id = instance.serviceEndpoint.id();
      ObjectName objectName =
          new ObjectName("io.scalecube.services:name=ScalecubeServiceDiscovery@" + id);
      StandardMBean standardMBean = new StandardMBean(jmxMBean, MonitorMBean.class);
      mbeanServer.registerMBean(standardMBean, objectName);
      return jmxMBean;
    }

    private void init() {
      discovery.listenDiscovery().subscribe(this::onDiscoveryEvent);
    }

    @Override
    public String getClusterConfig() {
      return String.valueOf(discovery.clusterConfig);
    }

    @Override
    public String getDiscoveryAddress() {
      return String.valueOf(discovery.address());
    }

    @Override
    public String getRecentDiscoveryEvents() {
      return recentDiscoveryEvents.stream()
          .map(ServiceDiscoveryEvent::toString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    private void onDiscoveryEvent(ServiceDiscoveryEvent event) {
      recentDiscoveryEvents.add(event);
      if (recentDiscoveryEvents.size() > RECENT_DISCOVERY_EVENTS_SIZE) {
        recentDiscoveryEvents.remove(0);
      }
    }
  }
}
