package io.scalecube.services.examples.interceptors;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTracingExample {

  public static final Logger LOGGER = LoggerFactory.getLogger(SimpleTracingExample.class);

  public static final Address SEED_ADDRESS = Address.create("localhost", 4444);

  static ServiceDiscovery seedDiscovery(ServiceEndpoint e) {
    return new ScalecubeServiceDiscovery(e)
        .options(c -> c.transport(t -> t.host(SEED_ADDRESS.host()).port(SEED_ADDRESS.port())));
  }

  static ServiceDiscovery fooDiscovery(ServiceEndpoint e) {
    return new ScalecubeServiceDiscovery(e)
        .options(c -> c.membership(m -> m.seedMembers(SEED_ADDRESS)));
  }

  static ServiceDiscovery barDiscovery(ServiceEndpoint e) {
    return new ScalecubeServiceDiscovery(e)
        .options(c -> c.membership(m -> m.seedMembers(SEED_ADDRESS)));
  }

  static ServiceDiscovery bazDiscovery(ServiceEndpoint e) {
    return new ScalecubeServiceDiscovery(e)
        .options(c -> c.membership(m -> m.seedMembers(SEED_ADDRESS)));
  }

  static ServiceMessage logRequest(ServiceMessage m) {
    LOGGER.info(">>> q: {}, sid: {}", m.header("q"), m.header("sid"));
    return m;
  }

  static ServiceMessage logResponse(ServiceMessage m) {
    LOGGER.info("<<< q: {}, sid: {}", m.header("q"), m.header("sid"));
    return m;
  }

  /**
   * Example runner.
   *
   * @param args program arguments.
   * @throws InterruptedException exception.
   */
  public static void main(String[] args) throws InterruptedException {
    Microservices seed =
        Microservices.builder()
            .discovery(SimpleTracingExample::seedDiscovery)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Microservices baz =
        Microservices.builder()
            .discovery(SimpleTracingExample::bazDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(ServiceInfo.fromServiceInstance(new ServiceBazImpl()).build())
            .requestMapper(SimpleTracingExample::logRequest)
            .responseMapper(SimpleTracingExample::logResponse)
            .startAwait();

    Microservices bar =
        Microservices.builder()
            .discovery(SimpleTracingExample::barDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(ServiceInfo.fromServiceInstance(new ServiceBarImpl()).build())
            .requestMapper(SimpleTracingExample::logRequest)
            .responseMapper(SimpleTracingExample::logResponse)
            .startAwait();

    Microservices foo =
        Microservices.builder()
            .discovery(SimpleTracingExample::fooDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(ServiceInfo.fromServiceInstance(new ServiceFooImpl()).build())
            .requestMapper(SimpleTracingExample::logRequest)
            .responseMapper(SimpleTracingExample::logResponse)
            .startAwait();

    ServiceMessage request =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString("example.service.foo", "foo"))
            .header("sid", 1)
            .data(5)
            .build();

    ServiceMessage response = foo.call().api(ServiceFoo.class).foo(request).block();

    LOGGER.info("### serviceFoo.foo({}) = {}", request.data(), response);

    Thread.currentThread().join();
  }
}
