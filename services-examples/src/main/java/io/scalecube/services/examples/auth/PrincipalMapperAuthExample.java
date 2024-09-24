package io.scalecube.services.examples.auth;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import reactor.core.publisher.Mono;

public class PrincipalMapperAuthExample {

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
                ServiceInfo.fromServiceInstance(new SecuredServiceByApiKeyImpl())
                    .principalMapper(PrincipalMapperAuthExample::apiKeyPrincipalMapper)
                    .build())
            .services(
                ServiceInfo.fromServiceInstance(new SecuredServiceByUserProfileImpl())
                    .principalMapper(PrincipalMapperAuthExample::userProfilePrincipalMapper)
                    .build())
            .startAwait();

    Microservices userProfileCaller =
        Microservices.builder()
            .discovery(endpoint -> discovery(service, endpoint))
            .transport(
                () -> new RSocketServiceTransport().credentialsSupplier(credentialsSupplier()))
            .startAwait();

    String responseByUserProfile =
        userProfileCaller
            .call()
            .api(SecuredServiceByUserProfile.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'userProfileCaller' response: " + responseByUserProfile);

    Microservices apiKeyCaller =
        Microservices.builder()
            .discovery(endpoint -> discovery(service, endpoint))
            .transport(
                () -> new RSocketServiceTransport().credentialsSupplier(credentialsSupplier()))
            .startAwait();

    String responseByApiKey =
        apiKeyCaller
            .call()
            .api(SecuredServiceByApiKey.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'apiKeyCaller' response: " + responseByApiKey);
  }

  private static Authenticator<Map<String, String>> authenticator() {
    return headers -> {
      String credsType = headers.get("type"); // credentials type

      if (SecuredServiceByUserProfile.class.getName().equals(credsType)) {
        return authenticateUserProfile(headers);
      }
      if (SecuredServiceByApiKey.class.getName().equals(credsType)) {
        return authenticateApiKey(headers);
      }
      throw new IllegalArgumentException(
          "[authenticator] not expected namespace: '" + credsType + "'");
    };
  }

  private static CredentialsSupplier credentialsSupplier() {
    return service -> {
      String namespace = service.namespace(); // decide by service

      if (SecuredServiceByUserProfile.class.getName().equals(namespace)) {
        return userProfileCredentials();
      }
      if (SecuredServiceByApiKey.class.getName().equals(namespace)) {
        return apiKeyCredentials();
      }
      throw new IllegalArgumentException(
          "[credentialsSupplier] not expected namespace: '" + namespace + "'");
    };
  }

  private static Mono<Map<String, String>> authenticateUserProfile(Map<String, String> headers) {
    String username = headers.get("username");
    String password = headers.get("password");

    if ("Alice".equals(username) && "qwerty".equals(password)) {
      HashMap<String, String> authData = new HashMap<>();
      authData.put("name", "Alice");
      authData.put("role", "ADMIN");
      return Mono.just(authData);
    }

    return Mono.error(
        new UnauthorizedException("Authentication failed (username or password incorrect)"));
  }

  private static Mono<Map<String, String>> authenticateApiKey(Map<String, String> headers) {
    String apiKey = headers.get("apiKey");

    if ("jasds8fjasdfjasd89fa4k9rjn7ahdfasduf".equals(apiKey)) {
      HashMap<String, String> authData = new HashMap<>();
      authData.put("id", "jasds8fjasdfjasd89fa4k9rjn7ahdfasduf");
      authData.put("permissions", "OPERATIONS:EVENTS:ACTIONS");
      return Mono.just(authData);
    }

    return Mono.error(new UnauthorizedException("Authentication failed (apiKey incorrect)"));
  }

  private static Mono<Map<String, String>> userProfileCredentials() {
    HashMap<String, String> creds = new HashMap<>();
    creds.put("type", SecuredServiceByUserProfile.class.getName());
    creds.put("username", "Alice");
    creds.put("password", "qwerty");
    return Mono.just(creds);
  }

  private static Mono<Map<String, String>> apiKeyCredentials() {
    HashMap<String, String> creds = new HashMap<>();
    creds.put("type", SecuredServiceByApiKey.class.getName());
    creds.put("apiKey", "jasds8fjasdfjasd89fa4k9rjn7ahdfasduf");
    return Mono.just(creds);
  }

  private static UserProfile userProfilePrincipalMapper(Map<String, String> authData) {
    return new UserProfile(authData.get("name"), authData.get("role"));
  }

  private static ApiKey apiKeyPrincipalMapper(Map<String, String> authData) {
    return new ApiKey(authData.get("id"), authData.get("permissions"));
  }

  private static ScalecubeServiceDiscovery discovery(
      Microservices service, ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery()
        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
        .options(opts -> opts.metadata(endpoint))
        .membership(opts -> opts.seedMembers(service.discoveryAddress().toString()));
  }
}
