package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Principal;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

class LocalCallRequestContextTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private static final Principal ADMIN =
      new Principal() {
        @Override
        public String role() {
          return "admin";
        }
      };

  private Microservices microservices;

  @BeforeEach
  void setUp() {
    microservices =
        Microservices.start(new Context().services(new OuterServiceImpl(), new InnerServiceImpl()));
  }

  @AfterEach
  void tearDown() {
    microservices.close();
  }

  @Test
  void calleeSeesItsOwnPathParamsAndHeaders() {
    assertEquals(
        "inner-value|inner-header|null", callOuter("v1/outer/callEcho/outer-value", null));
  }

  @Test
  void calleeWithoutDynamicQualifierDoesNotSeeCallerPathParams() {
    assertEquals("null", callOuter("v1/outer/callPlain/outer-value", null));
  }

  @Test
  void callerSuppliedPrincipalIsPropagatedToCallee() {
    // caller supplies a context with only a principal, as gateway session handlers do
    assertEquals(
        "inner-value|inner-header|admin", callOuter("v1/outer/callEcho/outer-value", ADMIN));
  }

  private String callOuter(String qualifier, Principal principal) {
    var response =
        microservices
            .call()
            .requestOne(
                ServiceMessage.builder().qualifier(qualifier).header("h", "outer-header").build(),
                String.class);
    if (principal != null) {
      response = response.contextWrite(new RequestContext().principal(principal));
    }
    return response.map(ServiceMessage::<String>data).block(TIMEOUT);
  }

  @Service("v1/outer")
  public interface OuterService {

    @ServiceMethod("callEcho/:outerParam")
    Mono<String> callEcho();

    @ServiceMethod("callPlain/:outerParam")
    Mono<String> callPlain();
  }

  @Service("v1/inner")
  public interface InnerService {

    @ServiceMethod("echo/:innerParam")
    Mono<String> echo();

    @ServiceMethod("plain")
    Mono<String> plain();
  }

  public static class OuterServiceImpl implements OuterService {

    private ServiceCall call;

    @AfterConstruct
    void conclude(Microservices microservices) {
      call = microservices.call();
    }

    @Override
    public Mono<String> callEcho() {
      return callInner("v1/inner/echo/inner-value");
    }

    @Override
    public Mono<String> callPlain() {
      return callInner("v1/inner/plain");
    }

    private Mono<String> callInner(String qualifier) {
      return call.requestOne(
              ServiceMessage.builder().qualifier(qualifier).header("h", "inner-header").build(),
              String.class)
          .map(ServiceMessage::data);
    }
  }

  public static class InnerServiceImpl implements InnerService {

    @Override
    public Mono<String> echo() {
      return RequestContext.deferContextual()
          .map(
              context ->
                  context.pathParams().getString("innerParam")
                      + "|"
                      + context.header("h")
                      + "|"
                      + context.principal().role());
    }

    @Override
    public Mono<String> plain() {
      return RequestContext.deferContextual()
          .map(context -> String.valueOf(context.pathParams().getString("outerParam")));
    }
  }
}
