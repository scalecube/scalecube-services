package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.RequestContext;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class HttpGatewayAuthenticatorTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);
  private static final String USERNAME_PRINCIPAL_HEADER = "USERNAME_PRINCIPAL_HEADER";
  private static final String PASSWORD_PRINCIPAL_HEADER = "PASSWORD_PRINCIPAL_HEADER";

  private Microservices gateway;
  private Microservices microservices;
  private final HttpGatewayAuthenticator authenticator = mock(HttpGatewayAuthenticator.class);
  private ServiceCall serviceCall;
  private ServiceBehindGateway serviceBehindGateway;

  @BeforeEach
  void beforeEach() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(
                    () -> HttpGateway.builder().id("HTTP").authenticator(authenticator).build()));

    microservices =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts -> opts.seedMembers(gateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .defaultLogger("microservices")
                .services(new ServiceBehindGatewayImpl()));

    final var gatewayAddress = gateway.gateway("HTTP").address();
    final var router = StaticAddressRouter.forService(gatewayAddress, "app-service").build();

    serviceCall =
        new ServiceCall()
            .router(router)
            .transport(HttpGatewayClientTransport.builder().address(gatewayAddress).build());
    serviceBehindGateway = serviceCall.api(ServiceBehindGateway.class);
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
    if (gateway != null) {
      gateway.close();
    }
    if (microservices != null) {
      microservices.close();
    }
  }

  @Test
  void shouldAuthenticateSuccessfully() {
    final var username = "admin";
    final var password = "admin1234";
    when(authenticator.authenticate(any()))
        .thenReturn(
            Mono.just(
                Map.of(USERNAME_PRINCIPAL_HEADER, username, PASSWORD_PRINCIPAL_HEADER, password)));

    StepVerifier.create(serviceBehindGateway.hello("hello"))
        .assertNext(
            response -> {
              assertEquals(username, response.username(), "username");
              assertEquals(password, response.password(), "password");
            })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  void testAuthenticationFailed() {
    when(authenticator.authenticate(any()))
        .thenReturn(Mono.error(new UnauthorizedException("Authentication failed")));

    StepVerifier.create(serviceBehindGateway.hello("hello"))
        .consumeErrorWith(
            ex -> {
              final var exception = (UnauthorizedException) ex;
              assertEquals(401, exception.errorCode(), "errorCode");
              assertEquals("Authentication failed", exception.getMessage(), "errorMessage");
            })
        .verify(TIMEOUT);
  }

  @Service("serviceBehindGateway")
  public interface ServiceBehindGateway {

    @ServiceMethod
    Mono<HelloBehindGatewayResponse> hello(String name);
  }

  public static class ServiceBehindGatewayImpl implements ServiceBehindGateway {

    @Override
    public Mono<HelloBehindGatewayResponse> hello(String name) {
      return RequestContext.deferContextual()
          .map(
              context -> {
                final var headers = context.headers();
                final var username = headers.get(USERNAME_PRINCIPAL_HEADER);
                final var password = headers.get(PASSWORD_PRINCIPAL_HEADER);
                return new HelloBehindGatewayResponse(username, password);
              });
    }
  }

  public static class HelloBehindGatewayResponse {

    private String username;
    private String password;

    public HelloBehindGatewayResponse() {}

    public HelloBehindGatewayResponse(String username, String password) {
      this.username = username;
      this.password = password;
    }

    public String username() {
      return username;
    }

    public String password() {
      return password;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", HelloBehindGatewayResponse.class.getSimpleName() + "[", "]")
          .add("username='" + username + "'")
          .add("password='" + password + "'")
          .toString();
    }
  }
}
