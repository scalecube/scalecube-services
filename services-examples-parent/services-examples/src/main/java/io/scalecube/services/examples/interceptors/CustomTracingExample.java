package io.scalecube.services.examples.interceptors;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomTracingExample {

  public static final Logger LOGGER = LoggerFactory.getLogger(CustomTracingExample.class);

  /**
   * Example runner.
   *
   * @param args program arguments.
   * @throws Exception exception.
   */
  public static void main(String[] args) throws Exception {
    Microservices ms =
        Microservices.builder()
            .services(
                ServiceInfo.fromServiceInstance(new ServiceBazImpl())
                    .requestMapper(CustomTracingExample::customLogRequest)
                    .responseMapper(CustomTracingExample::customLogResponse)
                    .build())
            .services(ServiceInfo.fromServiceInstance(new ServiceBarImpl()).build())
            .services(ServiceInfo.fromServiceInstance(new ServiceFooImpl()).build())
            .requestMapper(CustomTracingExample::logRequest)
            .responseMapper(CustomTracingExample::logResponse)
            .startAwait();

    ServiceMessage request =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString("example.service.foo", "foo"))
            .header("sid", 1)
            .data(5)
            .build();

    ServiceMessage response = ms.call().api(ServiceFoo.class).foo(request).block();

    LOGGER.info("### serviceFoo.foo({}) = {}", request.data(), response);

    Thread.currentThread().join();
  }

  static ServiceMessage logRequest(ServiceMessage m) {
    LOGGER.info(">>> {}", m);
    return m;
  }

  static ServiceMessage logResponse(ServiceMessage m) {
    LOGGER.info("<<< {}", m);
    return m;
  }

  static ServiceMessage customLogRequest(ServiceMessage m) {
    LOGGER.info("REQ {} -> q: {}, d: {}", m.header("sid"), m.qualifier(), m.data());
    return m;
  }

  static ServiceMessage customLogResponse(ServiceMessage m) {
    LOGGER.info("RESP {} <- q: {}, d: {}", m.header("sid"), m.qualifier(), m.data());
    return m;
  }
}
