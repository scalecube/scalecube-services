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
import io.scalecube.services.ServiceGroup;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final Logger LOGGER_GROUP =
      LoggerFactory.getLogger("io.scalecube.services.discovery.ServiceGroupDiscovery");

  private final ServiceEndpoint serviceEndpoint;

  private ClusterConfig clusterConfig;

  private Cluster cluster;

  private Map<ServiceGroup, Collection<ServiceEndpoint>> groups = new HashMap<>();
  private Map<ServiceGroup, Integer> addedGroups = new HashMap<>();

  private final DirectProcessor<ServiceDiscoveryEvent> subject = DirectProcessor.create();
  private final FluxSink<ServiceDiscoveryEvent> sink = subject.sink();

  /**
   * Constructor.
   *
   * @param serviceEndpoint service endpoint
   */
  public ScalecubeServiceDiscovery(ServiceEndpoint serviceEndpoint) {
    this.serviceEndpoint = serviceEndpoint;

    // Add myself to the group if 'groupness' is defined
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();
    if (serviceGroup != null) {
      if (serviceGroup.size() == 0) {
        throw new IllegalArgumentException("serviceGroup is invalid, size can't be 0");
      }
      addToGroup(serviceGroup, serviceEndpoint);
    }

    clusterConfig =
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
    this.groups = other.groups;
    this.addedGroups = other.addedGroups;
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
                        ScalecubeServiceDiscovery.this.onMembershipEvent(event, sink);
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

  private void onMembershipEvent(
      MembershipEvent membershipEvent, FluxSink<ServiceDiscoveryEvent> sink) {

    ServiceDiscoveryEvent discoveryEvent = toServiceDiscoveryEvent(membershipEvent);

    if (discoveryEvent == null) {
      LOGGER.info("onMembershipEvent: {}", membershipEvent);
      return;
    }

    LOGGER.info(
        "onMembershipEvent: {}, service endpoint: {}",
        membershipEvent,
        discoveryEvent.serviceEndpoint().id());

    // publish discovery event
    sink.next(discoveryEvent);

    // handle groups and publish group discovery event, if needed
    if (discoveryEvent.serviceEndpoint().serviceGroup() != null) {
      onDiscoveryEvent(discoveryEvent, sink);
    }
  }

  private void onDiscoveryEvent(
      ServiceDiscoveryEvent discoveryEvent, FluxSink<ServiceDiscoveryEvent> sink) {

    ServiceEndpoint serviceEndpoint = discoveryEvent.serviceEndpoint();
    ServiceGroup serviceGroup = serviceEndpoint.serviceGroup();

    ServiceDiscoveryEvent groupDiscoveryEvent = null;
    String groupId = serviceGroup.id();

    // handle add to group
    if (discoveryEvent.isEndpointAdded()) {
      boolean isGroupAdded = addToGroup(serviceGroup, serviceEndpoint);
      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      LOGGER_GROUP.info(
          "Added service endpoint {} to group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      // publish event regardless of isGroupAdded result
      sink.next(ServiceDiscoveryEvent.newEndpointAddedToGroup(groupId, serviceEndpoint, endpoints));

      if (isGroupAdded) {
        LOGGER_GROUP.info("Service group {} added to the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceDiscoveryEvent.newGroupAdded(groupId, endpoints);
      }
    }

    // handle removal from group
    if (discoveryEvent.isEndpointLeaving() || discoveryEvent.isEndpointRemoved()) {
      if (!removeFromGroup(serviceGroup, serviceEndpoint)) {
        LOGGER_GROUP.warn(
            "Failed to remove service endpoint {} from group {}, "
                + "there were no such group or service endpoint was never registered in group",
            serviceEndpoint.id(),
            groupId);
        return;
      }

      Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(serviceGroup);

      LOGGER_GROUP.info(
          "Removed service endpoint {} from group {} (size now {})",
          serviceEndpoint.id(),
          groupId,
          endpoints.size());

      sink.next(
          ServiceDiscoveryEvent.newEndpointRemovedFromGroup(groupId, serviceEndpoint, endpoints));

      if (endpoints.isEmpty()) {
        LOGGER_GROUP.info("Service group {} removed from the cluster", serviceGroup);
        groupDiscoveryEvent = ServiceDiscoveryEvent.newGroupRemoved(groupId);
      }
    }

    // publish group event
    if (groupDiscoveryEvent != null) {
      sink.next(groupDiscoveryEvent);
    }
  }

  public Collection<ServiceEndpoint> getEndpointsFromGroup(ServiceGroup group) {
    return groups.getOrDefault(group, Collections.emptyList());
  }

  /**
   * Adds service endpoint to the group and returns indication whether group is fully formed.
   *
   * @param group service group
   * @param endpoint service ednpoint
   * @return {@code true} if group is fully formed; {@code false} otherwise, for example when
   *     there's not enough members yet or group was already formed and just keep updating
   */
  private boolean addToGroup(ServiceGroup group, ServiceEndpoint endpoint) {
    Collection<ServiceEndpoint> endpoints =
        groups.computeIfAbsent(group, group1 -> new ArrayList<>());
    endpoints.add(endpoint);

    int size = group.size();
    if (size == 1) {
      return addedGroups.putIfAbsent(group, 1) == null;
    }

    if (addedGroups.computeIfAbsent(group, group1 -> 0) == size) {
      return false;
    }

    int countAfter = addedGroups.compute(group, (group1, count) -> count + 1);
    return countAfter == size;
  }

  /**
   * Removes service endpoint from group.
   *
   * @param group service group
   * @param endpoint service endpoint
   * @return {@code true} if endpoint was removed from group; {@code false} if group didn't exist or
   *     endpoint wasn't contained in the group
   */
  private boolean removeFromGroup(ServiceGroup group, ServiceEndpoint endpoint) {
    if (!groups.containsKey(group)) {
      return false;
    }
    Collection<ServiceEndpoint> endpoints = getEndpointsFromGroup(group);
    boolean removed = endpoints.removeIf(input -> input.id().equals(endpoint.id()));
    if (removed && endpoints.isEmpty()) {
      groups.remove(group); // cleanup
      addedGroups.remove(group); // cleanup
    }
    return removed;
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

  public interface MonitorMBean {

    String getClusterConfig();

    String getDiscoveryAddress();

    String getAddedServiceGroups();

    String getAllServiceGroups();

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
    public String getAddedServiceGroups() {
      return discovery.addedGroups.entrySet().stream()
          .map(entry -> toServiceGroupString(entry.getKey(), entry.getValue()))
          .collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public String getAllServiceGroups() {
      return discovery.groups.entrySet().stream()
          .map(entry -> toServiceGroupString(entry.getKey(), entry.getValue()))
          .collect(Collectors.joining(",", "[", "]"));
    }

    @Override
    public String getRecentDiscoveryEvents() {
      return recentDiscoveryEvents.stream()
          .map(ServiceDiscoveryEvent::toString)
          .collect(Collectors.joining(",", "[", "]"));
    }

    private String toServiceGroupString(ServiceGroup serviceGroup, int count) {
      String id = serviceGroup.id();
      int size = serviceGroup.size();
      return id + ":" + size + "/count=" + count;
    }

    private String toServiceGroupString(
        ServiceGroup serviceGroup, Collection<ServiceEndpoint> endpoints) {
      String id = serviceGroup.id();
      int size = serviceGroup.size();
      return id + ":" + size + "/endpoints=" + endpoints.size();
    }

    private void onDiscoveryEvent(ServiceDiscoveryEvent event) {
      recentDiscoveryEvents.add(event);
      if (recentDiscoveryEvents.size() > RECENT_DISCOVERY_EVENTS_SIZE) {
        recentDiscoveryEvents.remove(0);
      }
    }
  }
}
