package io.scalecube.services.examples.discovery;

import static java.util.stream.Collectors.toSet;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceSubscriber;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Follows one cluster of nodes with {@link ServiceSubscriber}, and fails over to another cluster
 * when the followed one is gone.
 *
 * <p>Each node publishes its cluster name in the {@code cluster} tag. Cluster {@code a} has two
 * nodes, cluster {@code b} has one. The subscriber follows {@code a} (the lowest key), keeps
 * following it while one of its nodes is alive, and switches to {@code b} when the last one stops.
 */
public class ServiceSubscriberExample {

  private static final String CLUSTER_TAG = "cluster";

  /**
   * Main method.
   *
   * @param args program arguments
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    final BlockingQueue<String> events = new LinkedBlockingQueue<>();

    final ServiceSubscriber<ClusterClient> subscriber =
        ServiceSubscriber.<ClusterClient>builder()
            .name("clusterClient")
            .filter(endpoint -> endpoint.tags().containsKey(CLUSTER_TAG))
            .groupBy(endpoint -> endpoint.tags().get(CLUSTER_TAG))
            .onAdded(
                member -> {
                  // runs on the discovery thread: must not block
                  final var client = new ClusterClient(member.tags().get(CLUSTER_TAG), member.id());
                  events.add("connected to " + client);
                  return client;
                })
            .onRemoved(
                (member, client) -> {
                  client.close();
                  events.add("disconnected from " + client);
                })
            .build();

    final Context context = context(null, null);
    // subscribe before start, otherwise endpoints already known are not seen
    context.listenDiscovery().subscribe(subscriber);
    final Microservices node = Microservices.start(context);

    final Microservices a0 = member(node, "a");
    System.out.println(events.take());

    final Microservices a1 = member(node, "a");
    final Microservices b0 = member(node, "b");
    awaitDiscovered(node, a1, b0);

    a0.close(); // cluster a still has a1: nothing happens
    a1.close(); // cluster a is gone: fails over to b
    System.out.println(events.take());
    System.out.println(events.take());

    subscriber.close(); // disconnects from b
    System.out.println(events.take());

    b0.close();
    node.close();
  }

  // the registry is updated before the event reaches the subscriber, in the same ordered stream
  private static void awaitDiscovered(Microservices node, Microservices... members)
      throws InterruptedException {
    final Set<String> ids = Arrays.stream(members).map(Microservices::id).collect(toSet());
    while (!node.serviceRegistry().listServiceEndpoints().stream()
        .map(ServiceEndpoint::id)
        .collect(toSet())
        .containsAll(ids)) {
      Thread.sleep(10);
    }
  }

  private static Microservices member(Microservices seed, String cluster) {
    return Microservices.start(context(seed, cluster).services(new GreetingServiceImpl()));
  }

  private static Context context(Microservices seed, String cluster) {
    final Context context =
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(
                            cfg ->
                                seed != null
                                    ? cfg.seedMembers(seed.discoveryAddress().toString())
                                    : cfg))
            .transport(RSocketServiceTransport::new);
    return cluster != null ? context.tags(Map.of(CLUSTER_TAG, cluster)) : context;
  }

  // stands for a real client of the cluster: a connection, a replay, a persister
  private record ClusterClient(String cluster, String memberId) implements AutoCloseable {

    @Override
    public void close() {}

    @Override
    public String toString() {
      return "cluster " + cluster + " via " + memberId;
    }
  }
}
