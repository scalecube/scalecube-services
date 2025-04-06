package io.scalecube.services.gateway.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.AuthRegistry;
import io.scalecube.services.gateway.SecuredService;
import io.scalecube.services.gateway.SecuredServiceImpl;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

public class WebsocketGatewayAuthTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final String ALLOWED_USER = "ALICE";
  private static final AuthRegistry AUTH_REGISTRY = new AuthRegistry(Set.of(ALLOWED_USER));

  private static Microservices gateway;

  private ServiceCall serviceCall;
  private SecuredService securedService;

  @BeforeAll
  static void beforeAll() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(
                    () ->
                        WebsocketGateway.builder()
                            .id("WS")
                            .gatewayHandler(new GatewaySessionHandlerImpl(AUTH_REGISTRY))
                            .build())
                .defaultLogger("gateway")
                .services(new SecuredServiceImpl(AUTH_REGISTRY)));
  }

  @BeforeEach
  void beforeEach() {
    final Address gatewayAddress = gateway.gateway("WS").address();

    serviceCall =
        new ServiceCall()
            .router(StaticAddressRouter.from(gatewayAddress).serviceName("app-service").build())
            .transport(WebsocketGatewayClientTransport.builder().address(gatewayAddress).build());

    securedService = serviceCall.api(SecuredService.class);
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @AfterAll
  static void afterAll() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void createSessionSuccessfully() {
    StepVerifier.create(serviceCall.requestOne(createSessionRequest(ALLOWED_USER), String.class))
        .expectNextCount(1)
        .expectComplete()
        .verify();
  }

  @Test
  void createSessionForbiddenUser() {
    StepVerifier.create(
            serviceCall.requestOne(createSessionRequest("fake" + ALLOWED_USER), String.class))
        .expectErrorSatisfies(
            th -> {
              UnauthorizedException e = (UnauthorizedException) th;
              assertEquals(401, e.errorCode(), "error code");
              assertEquals("Authentication failed", e.getMessage());
            })
        .verify();
  }

  @Test
  void securedMethodNotAuthenticated() {
    StepVerifier.create(securedService.requestOne("echo"))
        .expectErrorSatisfies(
            th -> {
              ForbiddenException e = (ForbiddenException) th;
              assertEquals(403, e.errorCode(), "errorCode");
              assertEquals("Insufficient permissions", e.getMessage(), "errorMessage");
            })
        .verify();
  }

  @Test
  void securedMethodAuthenticated() {
    // authenticate session
    serviceCall.requestOne(createSessionRequest(ALLOWED_USER), String.class).block(TIMEOUT);
    // call secured service
    final String req = "echo";
    StepVerifier.create(securedService.requestOne(req))
        .expectNextMatches(resp -> resp.equals(ALLOWED_USER + "@" + req))
        .expectComplete()
        .verify();
  }

  @Test
  void securedMethodAuthenticatedInvalidUser() {
    // authenticate session
    StepVerifier.create(
            serviceCall.requestOne(createSessionRequest("fake" + ALLOWED_USER), String.class))
        .expectErrorSatisfies(th -> assertInstanceOf(UnauthorizedException.class, th))
        .verify();
    // call secured service
    final String req = "echo";
    StepVerifier.create(securedService.requestOne(req))
        .expectErrorSatisfies(
            th -> {
              ForbiddenException e = (ForbiddenException) th;
              assertEquals(403, e.errorCode(), "errorCode");
              assertEquals("Insufficient permissions", e.getMessage(), "errorMessage");
            })
        .verify();
  }

  @Test
  void securedMethodNotAuthenticatedRequestStream() {
    StepVerifier.create(securedService.requestMany(10))
        .expectErrorSatisfies(
            th -> {
              ForbiddenException e = (ForbiddenException) th;
              assertEquals(403, e.errorCode(), "errorCode");
              assertEquals("Insufficient permissions", e.getMessage(), "errorMessage");
            })
        .verify();
  }

  @Test
  void securedMethodAuthenticatedReqStream() {
    // authenticate session
    serviceCall.requestOne(createSessionRequest(ALLOWED_USER), String.class).block(TIMEOUT);
    // call secured service
    Integer times = 10;
    StepVerifier.create(securedService.requestMany(times))
        .expectNextCount(10)
        .expectComplete()
        .verify();
  }

  private static ServiceMessage createSessionRequest(String username) {
    return ServiceMessage.builder()
        .qualifier(Qualifier.asString(SecuredService.NAMESPACE, "createSession"))
        .data(username)
        .build();
  }
}
