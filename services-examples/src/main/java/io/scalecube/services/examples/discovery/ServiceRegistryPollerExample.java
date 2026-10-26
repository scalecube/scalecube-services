package io.scalecube.services.examples.discovery;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.GreetingServiceImpl;
import io.scalecube.services.registry.api.ServiceRegistryPoller;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Keeps a copy of the service endpoints with {@link ServiceRegistryPoller}, polled from one thread,
 * e.g. the duty cycle of an agent, instead of reacting to discovery events.
 *
 * <p>The observer node polls its registry while two other nodes start and one of them stops. A poll
 * without changes costs a read of the registry version and nothing else.
 */
public class ServiceRegistryPollerExample {

  /**
   * Main method.
   *
   * @param args program arguments
   * @throws Exception exception
   */
  public static void main(String[] args) throws Exception {
    final Microservices observer = Microservices.start(context(null));

    // a poller belongs to the thread that polls it
    final ServiceRegistryPoller poller = observer.serviceRegistry().poller();
    final Map<String, ServiceEndpoint> endpoints = new HashMap<>();

    final Microservices node1 = Microservices.start(context(observer));
    final Microservices node2 = Microservices.start(context(observer));

    pollUntil(poller, endpoints, map -> map.containsKey(node1.id()) && map.containsKey(node2.id()));

    node1.close();
    pollUntil(poller, endpoints, map -> !map.containsKey(node1.id()));

    // without callbacks: poll() tells whether something changed, endpoints() is the current view
    if (poller.poll() == 0) {
      System.out.println("no changes, endpoints: " + poller.endpoints().size());
    }

    node2.close();
    observer.close();
  }

  private static void pollUntil(
      ServiceRegistryPoller poller,
      Map<String, ServiceEndpoint> endpoints,
      Predicate<Map<String, ServiceEndpoint>> condition)
      throws InterruptedException {
    while (!condition.test(endpoints)) {
      final int changes =
          poller.poll(
              endpoint -> {
                endpoints.put(endpoint.id(), endpoint);
                System.out.println("added " + endpoint.id());
              },
              endpoint -> {
                endpoints.remove(endpoint.id());
                System.out.println("removed " + endpoint.id());
              });
      if (changes == 0) {
        Thread.sleep(10); // idle strategy of the polling thread
      }
    }
  }

  private static Context context(Microservices seed) {
    return new Context()
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
        .transport(RSocketServiceTransport::new)
        .services(new GreetingServiceImpl());
  }
}
