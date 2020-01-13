package io.scalecube.services.discovery;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.cluster.transport.api.MessageCodec;
import io.scalecube.net.Address;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

  private static final Logger LOGGER =
      LoggerFactory.getLogger("io.scalecube.services.discovery.ServiceDiscovery");

  private final ServiceEndpoint serviceEndpoint;

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
    this.clusterConfig =
        ClusterConfig.defaultLanConfig()
            .metadata(serviceEndpoint)
            .transport(config -> config.messageCodec(new MessageCodecImpl()))
            .metadataEncoder(this::encode)
            .metadataDecoder(this::decode);
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
   * Setter for {@code ClusterConfig.Builder} options.
   *
   * @param opts ClusterConfig options builder
   * @return new instance of {@code ScalecubeServiceDiscovery}
   */
  public ScalecubeServiceDiscovery options(UnaryOperator<ClusterConfig> opts) {
    ScalecubeServiceDiscovery d = new ScalecubeServiceDiscovery(this);
    d.clusterConfig = opts.apply(clusterConfig);
    return d;
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
      ServiceEndpoint serviceEndpoint = (ServiceEndpoint) decode(membershipEvent.newMetadata());
      discoveryEvent = ServiceDiscoveryEvent.newEndpointAdded(serviceEndpoint);
    }

    if (membershipEvent.isRemoved() && membershipEvent.oldMetadata() != null) {
      ServiceEndpoint serviceEndpoint = (ServiceEndpoint) decode(membershipEvent.oldMetadata());
      discoveryEvent = ServiceDiscoveryEvent.newEndpointRemoved(serviceEndpoint);
    }

    if (membershipEvent.isLeaving() && membershipEvent.newMetadata() != null) {
      ServiceEndpoint serviceEndpoint = (ServiceEndpoint) decode(membershipEvent.newMetadata());
      discoveryEvent = ServiceDiscoveryEvent.newEndpointLeaving(serviceEndpoint);
    }

    return discoveryEvent;
  }

  private Object decode(ByteBuffer byteBuffer) {
    try {
      return DefaultObjectMapper.OBJECT_MAPPER.readValue(
          new ByteBufferBackedInputStream(byteBuffer), ServiceEndpoint.class);
    } catch (IOException e) {
      LOGGER.error("Failed to read metadata: " + e);
      return null;
    }
  }

  private ByteBuffer encode(Object input) {
    ServiceEndpoint serviceEndpoint = (ServiceEndpoint) input;
    try {
      return ByteBuffer.wrap(
          DefaultObjectMapper.OBJECT_MAPPER
              .writeValueAsString(serviceEndpoint)
              .getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOGGER.error("Failed to write metadata: " + e);
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

  private static class MessageCodecImpl implements MessageCodec {

    @Override
    public Message deserialize(InputStream stream) throws Exception {
      return DefaultObjectMapper.OBJECT_MAPPER.readValue(stream, Message.class);
    }

    @Override
    public void serialize(Message message, OutputStream stream) throws Exception {
      DefaultObjectMapper.OBJECT_MAPPER.writeValue(stream, message);
    }
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
