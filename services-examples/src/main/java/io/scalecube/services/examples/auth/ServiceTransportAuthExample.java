package io.scalecube.services.examples.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
import io.scalecube.services.transport.api.ServerTransport.Authenticator;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import reactor.core.publisher.Mono;

public class ServiceTransportAuthExample {

  /**
   * Main program.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    Microservices service =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(() -> new RSocketServiceTransport().authenticator(authenticator()))
                .services(new SecuredServiceByUserProfileImpl()));

    Microservices caller =
        Microservices.start(
            new Context()
                .discovery(endpoint -> discovery(service, endpoint))
                .transport(
                    () ->
                        new RSocketServiceTransport().credentialsSupplier(credentialsSupplier())));

    String response =
        caller
            .call()
            .api(SecuredServiceByUserProfile.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'caller' response: " + response);
  }

  private static Authenticator authenticator() {
    return credentials -> {
      final var headers = credentials(credentials);

      String username = headers.get("username");
      String password = headers.get("password");

      if ("Alice".equals(username) && "qwerty".equals(password)) {
        HashMap<String, String> authData = new HashMap<>();
        authData.put("name", "Alice");
        authData.put("role", "ADMIN");
        return Mono.just(new UserProfile(authData.get("name"), authData.get("role")));
      }

      return Mono.error(
          new UnauthorizedException("Authentication failed (username or password incorrect)"));
    };
  }

  private static CredentialsSupplier credentialsSupplier() {
    return (serviceReference, serviceRole) -> {
      HashMap<String, String> credentials = new HashMap<>();
      credentials.put("username", "Alice");
      credentials.put("password", "qwerty");
      return Mono.just(encodeCredentials(credentials));
    };
  }

  private static ScalecubeServiceDiscovery discovery(
      Microservices service, ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(endpoint))
        .membership(opts -> opts.seedMembers(service.discoveryAddress().toString()));
  }

  private static byte[] encodeCredentials(Map<String, String> credentials) {
    try {
      return new JsonMapper().writeValueAsBytes(credentials);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> credentials(byte[] credentials) {
    final Map<String, String> headers;
    try {
      //noinspection unchecked
      headers = new JsonMapper().readValue(credentials, HashMap.class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return headers;
  }
}
