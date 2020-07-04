package io.scalecube.services.examples.auth;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
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
            .discovery("service", ScalecubeServiceDiscovery::new)
            .transport(() -> new RSocketServiceTransport().authenticator(authenticator()))
            .services(ServiceInfo.fromServiceInstance(new SecuredServiceImpl()).build())
            .startAwait();

    Microservices caller =
        Microservices.builder()
            .discovery("caller", endpoint -> discovery(service, endpoint))
            .transport(() -> new RSocketServiceTransport().credentialsSupplier(credsSupplier()))
            .startAwait();

    String hello =
        caller
            .call()
            .api(SecuredService.class)
            .securedHello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'secured hello' response: " + hello);
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

  private static CredentialsSupplier credsSupplier() {
    return service -> {
      HashMap<String, String> creds = new HashMap<>();
      creds.put("username", "Alice");
      creds.put("password", "qwerty");
      return Mono.just(creds);
    };
  }

  private static ScalecubeServiceDiscovery discovery(
      Microservices service, ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery(endpoint)
        .membership(opts -> opts.seedMembers(service.discovery("service").address()));
  }
}
