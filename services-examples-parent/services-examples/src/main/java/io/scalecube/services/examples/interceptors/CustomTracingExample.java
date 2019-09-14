package io.scalecube.services.examples.interceptors;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.UnauthorizedException;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

public class CustomTracingExample {

  public static final Logger LOGGER = LoggerFactory.getLogger(CustomTracingExample.class);

  private static final Map<String, String> CREDENTIALS =
      new HashMap<String, String>() {
        {
          put("username", "Alice");
          put("password", "qwerty");
        }
      };

  /**
   * Example runner.
   *
   * @param args program arguments.
   * @throws Exception exception.
   */
  public static void main(String[] args) throws Exception {
    Microservices ms =
        Microservices.builder()
            .authenticator(CustomTracingExample::authenticate)
            .services(ServiceInfo.fromServiceInstance(new ServiceBazImpl()).build())
            .services(ServiceInfo.fromServiceInstance(new ServiceBarImpl()).build())
            .services(ServiceInfo.fromServiceInstance(new ServiceFooImpl()).build())
            .services(ServiceInfo.fromServiceInstance(new SecuredServiceFooImpl()).build())
            .requestMapper(CustomTracingExample::logRequest)
            .responseMapper(CustomTracingExample::logResponse)
            .startAwait();

    LOGGER.info("### Calling foo.call().api(ServiceFoo.class) method 'foo'");

    ServiceMessage request =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString("example.service.foo", "foo"))
            .header("sid", 1)
            .data(5)
            .build();

    ServiceMessage response = ms.call().api(ServiceFoo.class).foo(request).block();

    LOGGER.info("### serviceFoo.foo({}) = {}", request.data(), response);

    LOGGER.info("### Calling foo.credentials().api(SecuredServiceFoo.class) method 'securedFoo'");

    ServiceMessage response1 =
        ms.call().credentials(CREDENTIALS).api(SecuredServiceFoo.class).securedFoo(request).block();

    LOGGER.info("### securedFoo.securedFoo({}) = {}", request.data(), response1);

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

  static ServiceMessage logSecuredRequest(ServiceMessage m) {
    LOGGER.info(">>> {}", m);
    return m;
  }

  static ServiceMessage logSecureResponse(ServiceMessage m) {
    LOGGER.info("<<< {}", m);
    return m;
  }

  static Mono<Object> authenticate(ServiceMessage m) {
    Map<String, String> headers = m.headers();
    String username = headers.get("username");
    String password = headers.get("password");

    if ("Alice".equals(username) && "qwerty".equals(password)) {
      return Mono.just(new UserProfile("Alice", "ADMIN"));
    }

    return Mono.error(new UnauthorizedException("Authentication failed"));
  }
}
