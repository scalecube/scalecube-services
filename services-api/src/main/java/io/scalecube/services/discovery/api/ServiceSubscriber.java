package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;

/**
 * Follows one service endpoint, or one group of them: filters the discovered endpoints, and calls
 * {@link Builder#onAdded(Function)} when the followed one appears and {@link
 * Builder#onRemoved(BiConsumer)} when it is gone, then fails over to another matching one.
 *
 * <p>Without {@link Builder#groupBy(Function)} every endpoint is its own group. With it, endpoints
 * of the same key form one group: it is added with its first member and removed when its last
 * member is gone. The followed group is the lowest key among the discovered ones.
 *
 * <p>An endpoint is gone on its first {@link ServiceDiscoveryEvent#isEndpointLeaving() leaving} or
 * {@link ServiceDiscoveryEvent#isEndpointRemoved() removed} event: a gracefully stopped node is
 * leaving right away, but removed only after the membership timeouts.
 *
 * <p>Whatever {@code onAdded} returns is handed back to {@code onRemoved}, with the same member.
 * Callbacks run on the thread delivering the discovery events, so they must not block. An exception
 * of a callback goes to {@link Builder#onError(Consumer)}.
 *
 * @param <R> what {@code onAdded} returns
 */
public final class ServiceSubscriber<R> extends BaseSubscriber<ServiceDiscoveryEvent>
    implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceSubscriber.class);

  private final String name;
  private final Predicate<ServiceEndpoint> filter;
  private final Function<ServiceEndpoint, String> groupBy;
  private final Function<ServiceEndpoint, R> onAdded;
  private final BiConsumer<ServiceEndpoint, R> onRemoved;
  private final Consumer<Throwable> onError;

  // group -> endpointId -> member, sorted to fail over to the same group every time
  private final TreeMap<String, Map<String, ServiceEndpoint>> groups = new TreeMap<>();

  private String current;
  private ServiceEndpoint currentMember;
  private R currentResource;

  private ServiceSubscriber(Builder<R> builder) {
    name = builder.name;
    filter = builder.filter;
    groupBy = builder.groupBy;
    onAdded = builder.onAdded;
    onRemoved = builder.onRemoved;
    onError = builder.onError;
  }

  /**
   * Returns new {@link Builder}.
   *
   * @param <R> what {@code onAdded} returns
   * @return builder
   */
  public static <R> Builder<R> builder() {
    return new Builder<>();
  }

  @Override
  protected synchronized void hookOnNext(ServiceDiscoveryEvent event) {
    final var endpoint = event.serviceEndpoint();
    if (!filter.test(endpoint)) {
      return;
    }
    final var group = groupBy.apply(endpoint);
    if (event.isEndpointAdded()) {
      groups.computeIfAbsent(group, k -> new LinkedHashMap<>()).put(endpoint.id(), endpoint);
    } else if (event.isEndpointLeaving() || event.isEndpointRemoved()) {
      final var members = groups.get(group);
      if (members == null || members.remove(endpoint.id()) == null) {
        return;
      }
      if (members.isEmpty()) {
        groups.remove(group);
      }
    } else {
      return;
    }
    try {
      if (current != null && !groups.containsKey(current)) {
        remove();
      }
      if (current == null && !groups.isEmpty()) {
        final var first = groups.firstEntry();
        final var member = first.getValue().values().iterator().next();
        LOGGER.info("[{}][added] {}", name, first.getKey());
        currentResource = onAdded.apply(member);
        currentMember = member;
        current = first.getKey();
      }
    } catch (Exception ex) {
      onError.accept(ex);
    }
  }

  private void remove() {
    LOGGER.info("[{}][removed] {}", name, current);
    final var member = currentMember;
    final var resource = currentResource;
    current = null;
    currentMember = null;
    currentResource = null;
    onRemoved.accept(member, resource);
  }

  /**
   * Cancels the subscription, then removes the followed one, calling {@code onRemoved}. Nothing is
   * followed after that, even if matching endpoints are still around.
   */
  @Override
  public synchronized void close() {
    dispose();
    try {
      if (current != null) {
        remove();
      }
    } catch (Exception ex) {
      onError.accept(ex);
    }
  }

  public static final class Builder<R> {

    private String name = "service";
    private Predicate<ServiceEndpoint> filter = endpoint -> true;
    private Function<ServiceEndpoint, String> groupBy = ServiceEndpoint::id;
    private Function<ServiceEndpoint, R> onAdded;
    private BiConsumer<ServiceEndpoint, R> onRemoved = (member, resource) -> {};
    private Consumer<Throwable> onError = ex -> LOGGER.error("Exception occurred", ex);

    private Builder() {}

    /**
     * Name in the logs. Default: {@code service}.
     *
     * @param name name
     * @return this builder
     */
    public Builder<R> name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Filter of the endpoints, applied to every discovery event: an endpoint that does not match is
     * ignored, as if never discovered. Default: all.
     *
     * @param filter filter
     * @return this builder
     */
    public Builder<R> filter(Predicate<ServiceEndpoint> filter) {
      this.filter = filter;
      return this;
    }

    /**
     * Group key of an endpoint. Must give the same key for the same endpoint on every event, it is
     * how a leaving member finds its group. Keys are compared as strings when choosing the lowest
     * one ({@code "10"} comes before {@code "9"}). Default: the endpoint id, every endpoint is its
     * own group.
     *
     * @param groupBy group key function
     * @return this builder
     */
    public Builder<R> groupBy(Function<ServiceEndpoint, String> groupBy) {
      this.groupBy = groupBy;
      return this;
    }

    /**
     * Called with the first member of the followed group when the group is added; the result goes
     * to {@code onRemoved}. Required. If it throws, the group is not followed, and the next
     * matching discovery event tries again.
     *
     * @param onAdded callback
     * @return this builder
     */
    public Builder<R> onAdded(Function<ServiceEndpoint, R> onAdded) {
      this.onAdded = onAdded;
      return this;
    }

    /**
     * Called with the member and the result of {@code onAdded} when the followed group is removed,
     * or on {@link ServiceSubscriber#close()}. The group is no longer followed even if it throws.
     * Default: no-op.
     *
     * @param onRemoved callback
     * @return this builder
     */
    public Builder<R> onRemoved(BiConsumer<ServiceEndpoint, R> onRemoved) {
      this.onRemoved = onRemoved;
      return this;
    }

    /**
     * Handler of an exception thrown by {@code onAdded} or {@code onRemoved}, e.g. to shut down
     * when a resource cannot be started. Default: logs it.
     *
     * @param onError handler
     * @return this builder
     */
    public Builder<R> onError(Consumer<Throwable> onError) {
      this.onError = onError;
      return this;
    }

    /**
     * Builds the subscriber, to be subscribed to the discovery events.
     *
     * @return subscriber
     */
    public ServiceSubscriber<R> build() {
      Objects.requireNonNull(filter, "filter");
      Objects.requireNonNull(groupBy, "groupBy");
      Objects.requireNonNull(onAdded, "onAdded");
      Objects.requireNonNull(onRemoved, "onRemoved");
      Objects.requireNonNull(onError, "onError");
      return new ServiceSubscriber<>(this);
    }
  }
}
