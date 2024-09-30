package io.scalecube.services.gateway.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.AuthRegistry;
import io.scalecube.services.gateway.SecuredService;
import io.scalecube.services.gateway.SecuredServiceImpl;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

public class WebsocketLocalGatewayAuthTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final String ALLOWED_USER = "VASYA_PUPKIN";
  private static final Set<String> ALLOWED_USERS =
      new HashSet<>(Collections.singletonList(ALLOWED_USER));

  private static final AuthRegistry AUTH_REG = new AuthRegistry(ALLOWED_USERS);

  @RegisterExtension
  static WebsocketLocalWithAuthExtension extension =
      new WebsocketLocalWithAuthExtension(new SecuredServiceImpl(AUTH_REG), AUTH_REG);

  private SecuredService clientService;

  private static ServiceMessage createSessionReq(String username) {
    return ServiceMessage.builder()
        .qualifier("/" + SecuredService.NAMESPACE + "/createSession")
        .data(username)
        .build();
  }

  @BeforeEach
  void initService() {
    clientService = extension.client().api(SecuredService.class);
  }

  @Test
  void testCreateSession_succ() {
    StepVerifier.create(extension.client().requestOne(createSessionReq(ALLOWED_USER), String.class))
        .expectNextCount(1)
        .expectComplete()
        .verify();
  }

  @Test
  void testCreateSession_forbiddenUser() {
    StepVerifier.create(
            extension.client().requestOne(createSessionReq("fake" + ALLOWED_USER), String.class))
        .expectErrorSatisfies(
            th -> {
              ForbiddenException e = (ForbiddenException) th;
              assertEquals(403, e.errorCode(), "error code");
              assertTrue(e.getMessage().contains("User not allowed to use this service"));
            })
        .verify();
  }

  @Test
  void testCallSecuredMethod_notAuthenticated() {
    StepVerifier.create(clientService.requestOne("echo"))
        .expectErrorSatisfies(
            th -> {
              UnauthorizedException e = (UnauthorizedException) th;
              assertEquals(401, e.errorCode(), "Authentication failed");
              assertTrue(e.getMessage().contains("Authentication failed"));
            })
        .verify();
  }

  @Test
  void testCallSecuredMethod_authenticated() {
    // authenticate session
    extension.client().requestOne(createSessionReq(ALLOWED_USER), String.class).block(TIMEOUT);
    // call secured service
    final String req = "echo";
    StepVerifier.create(clientService.requestOne(req))
        .expectNextMatches(resp -> resp.equals(ALLOWED_USER + "@" + req))
        .expectComplete()
        .verify();
  }

  @Test
  void testCallSecuredMethod_authenticatedInvalidUser() {
    // authenticate session
    StepVerifier.create(
            extension.client().requestOne(createSessionReq("fake" + ALLOWED_USER), String.class))
        .expectErrorSatisfies(th -> assertTrue(th instanceof ForbiddenException))
        .verify();
    // call secured service
    final String req = "echo";
    StepVerifier.create(clientService.requestOne(req))
        .expectErrorSatisfies(
            th -> {
              UnauthorizedException e = (UnauthorizedException) th;
              assertEquals(401, e.errorCode(), "Authentication failed");
              assertTrue(e.getMessage().contains("Authentication failed"));
            })
        .verify();
  }

  @Test
  void testCallSecuredMethod_notAuthenticatedRequestStream() {
    StepVerifier.create(clientService.requestN(10))
        .expectErrorSatisfies(
            th -> {
              UnauthorizedException e = (UnauthorizedException) th;
              assertEquals(401, e.errorCode(), "Authentication failed");
              assertTrue(e.getMessage().contains("Authentication failed"));
            })
        .verify();
  }

  @Test
  void testCallSecuredMethod_authenticatedReqStream() {
    // authenticate session
    extension.client().requestOne(createSessionReq(ALLOWED_USER), String.class).block(TIMEOUT);
    // call secured service
    Integer times = 10;
    StepVerifier.create(clientService.requestN(times))
        .expectNextCount(10)
        .expectComplete()
        .verify();
  }
}
