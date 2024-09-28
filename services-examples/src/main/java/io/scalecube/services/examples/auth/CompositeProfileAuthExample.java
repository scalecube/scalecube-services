package io.scalecube.services.examples.auth;

import static io.scalecube.services.auth.Authenticator.deferSecured;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceTransport.CredentialsSupplier;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.Collections;
import reactor.core.publisher.Mono;

public class CompositeProfileAuthExample {

  /**
   * Main program.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    Microservices service =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(() -> new RSocketServiceTransport().authenticator(authenticator()))
            .services(
                call ->
                    Collections.singletonList(
                        ServiceInfo.fromServiceInstance(new SecuredServiceByCompositeProfileImpl())
                            .authenticator(compositeAuthenticator())
                            .build()))
            .startAwait();

    Microservices caller =
        Microservices.builder()
            .discovery(endpoint -> discovery(service, endpoint))
            .transport(
                () -> new RSocketServiceTransport().credentialsSupplier(credentialsSupplier()))
            .startAwait();

    ServiceMessage response =
        caller
            .call()
            .requestOne(
                ServiceMessage.builder()
                    .qualifier("securedServiceByCompositeProfile/hello")
                    .header("userProfile.name", "SEGA")
                    .header("userProfile.role", "ADMIN")
                    .data("hello world")
                    .build(),
                String.class)
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'caller' response: " + response.data());
  }

  private static Authenticator<ServiceEndpointProfile> authenticator() {
    return headers -> {
      String transportSessionKey = headers.get("transportSessionKey");

      if ("asdf7hasd9hasd7fha8ds7fahsdf87".equals(transportSessionKey)) {
        return Mono.just(new ServiceEndpointProfile("endpoint123", "operations"));
      }

      return Mono.error(
          new UnauthorizedException("Authentication failed (transportSessionKey incorrect)"));
    };
  }

  private static CredentialsSupplier credentialsSupplier() {
    return service ->
        Mono.just(
            Collections.singletonMap("transportSessionKey", "asdf7hasd9hasd7fha8ds7fahsdf87"));
  }

  private static Authenticator<CompositeProfile> compositeAuthenticator() {
    return headers ->
        deferSecured(ServiceEndpointProfile.class)
            .flatMap(
                serviceEndpointProfile -> {

                  // If userProfile not set then throw error
                  if (!headers.containsKey("userProfile.name")
                      || !headers.containsKey("userProfile.role")) {
                    throw new UnauthorizedException("userProfile not found or invalid");
                  }

                  // Otherwise return new combined profile which will be stored under
                  // AUTH_CONTEXT_KEY

                  return Mono.just(
                      new CompositeProfile(
                          serviceEndpointProfile, UserProfile.fromHeaders(headers)));
                });
  }

  private static ScalecubeServiceDiscovery discovery(
      Microservices service, ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(endpoint))
        .membership(opts -> opts.seedMembers(service.discoveryAddress().toString()));
  }
}
