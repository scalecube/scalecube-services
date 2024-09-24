package io.scalecube.services.examples.auth;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceTransport.CredentialsSupplier;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.HashMap;
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
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(() -> new RSocketServiceTransport().authenticator(authenticator()))
            .services(new SecuredServiceByUserProfileImpl())
            .startAwait();

    Microservices caller =
        Microservices.builder()
            .discovery(endpoint -> discovery(service, endpoint))
            .transport(
                () -> new RSocketServiceTransport().credentialsSupplier(credentialsSupplier()))
            .startAwait();

    String response =
        caller
            .call()
            .api(SecuredServiceByUserProfile.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'caller' response: " + response);
  }

  private static Authenticator<UserProfile> authenticator() {
    return headers -> {
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
    return service -> {
      HashMap<String, String> creds = new HashMap<>();
      creds.put("username", "Alice");
      creds.put("password", "qwerty");
      return Mono.just(creds);
    };
  }

  private static ScalecubeServiceDiscovery discovery(
      Microservices service, ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(endpoint))
        .membership(opts -> opts.seedMembers(service.discoveryAddress().toString()));
  }
}
