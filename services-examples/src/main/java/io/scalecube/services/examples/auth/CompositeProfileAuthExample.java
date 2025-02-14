// package io.scalecube.services.examples.auth;
//
// import static io.scalecube.services.auth.Authenticator.deferSecured;
//
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.json.JsonMapper;
// import io.scalecube.services.Microservices;
// import io.scalecube.services.Microservices.Context;
// import io.scalecube.services.ServiceEndpoint;
// import io.scalecube.services.ServiceInfo;
// import io.scalecube.services.api.ServiceMessage;
// import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
// import io.scalecube.services.exceptions.UnauthorizedException;
// import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
// import io.scalecube.services.transport.api.ServerTransport.Authenticator;
// import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
// import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
// import java.io.IOException;
// import java.time.Duration;
// import java.util.Collections;
// import java.util.HashMap;
// import java.util.Map;
// import reactor.core.publisher.Mono;
//
// public class CompositeProfileAuthExample {
//
//  /**
//   * Main program.
//   *
//   * @param args arguments
//   */
//  public static void main(String[] args) {
//    Microservices service =
//        Microservices.start(
//            new Context()
//                .discovery(
//                    serviceEndpoint ->
//                        new ScalecubeServiceDiscovery()
//                            .transport(cfg -> cfg.transportFactory(new
// WebsocketTransportFactory()))
//                            .options(opts -> opts.metadata(serviceEndpoint)))
//                .transport(() -> new RSocketServiceTransport().authenticator(authenticator()))
//                .services(
//                    call ->
//                        Collections.singletonList(
//                            ServiceInfo.fromServiceInstance(
//                                    new SecuredServiceByCompositeProfileImpl())
//                                .authenticator(compositeAuthenticator())
//                                .build())));
//
//    Microservices caller =
//        Microservices.start(
//            new Context()
//                .discovery(endpoint -> discovery(service, endpoint))
//                .transport(
//                    () ->
//                        new
// RSocketServiceTransport().credentialsSupplier(credentialsSupplier())));
//
//    ServiceMessage response =
//        caller
//            .call()
//            .requestOne(
//                ServiceMessage.builder()
//                    .qualifier("securedServiceByCompositeProfile/hello")
//                    .header("userProfile.name", "SEGA")
//                    .header("userProfile.role", "ADMIN")
//                    .data("hello world")
//                    .build(),
//                String.class)
//            .block(Duration.ofSeconds(3));
//
//    System.err.println("### Received 'caller' response: " + response.data());
//  }
//
//  private static Authenticator authenticator() {
//    return credentials -> {
//      final var headers = credentials(credentials);
//
//      String transportSessionKey = headers.get("transportSessionKey");
//
//      if ("asdf7hasd9hasd7fha8ds7fahsdf87".equals(transportSessionKey)) {
//        return Mono.just(new PrincipalProfile("endpoint123", "operations", headers));
//      }
//
//      return Mono.error(
//          new UnauthorizedException("Authentication failed (transportSessionKey incorrect)"));
//    };
//  }
//
//  private static CredentialsSupplier credentialsSupplier() {
//    return (serviceReference, serviceRole) ->
//        Mono.just(
//            encodeCredentials(
//                Collections.singletonMap("transportSessionKey",
// "asdf7hasd9hasd7fha8ds7fahsdf87")));
//  }
//
//  private static io.scalecube.services.auth.Authenticator compositeAuthenticator() {
//    return deferSecured(PrincipalProfile.class)
//        .flatMap(
//            principalProfile -> {
//              final var headers = principalProfile.headers();
//
//              // If userProfile not set then throw error
//              if (!headers.containsKey("userProfile.name")
//                  || !headers.containsKey("userProfile.role")) {
//                throw new UnauthorizedException("userProfile not found or invalid");
//              }
//
//              // Otherwise return new combined profile which will be stored under
//              // AUTH_CONTEXT_KEY
//
//              return Mono.just(
//                  new CompositeProfile(principalProfile, UserProfile.fromHeaders(headers)));
//            });
//  }
//
//  private static ScalecubeServiceDiscovery discovery(
//      Microservices service, ServiceEndpoint endpoint) {
//    return new ScalecubeServiceDiscovery()
//        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
//        .options(opts -> opts.metadata(endpoint))
//        .membership(opts -> opts.seedMembers(service.discoveryAddress().toString()));
//  }
//
//  private static byte[] encodeCredentials(Map<String, String> credentials) {
//    try {
//      return new JsonMapper().writeValueAsBytes(credentials);
//    } catch (JsonProcessingException e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  private static Map<String, String> credentials(byte[] credentials) {
//    final Map<String, String> headers;
//    try {
//      //noinspection unchecked
//      headers = new JsonMapper().readValue(credentials, HashMap.class);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//    return headers;
//  }
// }
