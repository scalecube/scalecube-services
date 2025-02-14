package io.scalecube.services.examples.auth;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
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

public class PrincipalMapperAuthExample {

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
                .services(
                    ServiceInfo.fromServiceInstance(new SecuredServiceByApiKeyImpl())
                        .principalMapper(PrincipalMapperAuthExample::apiKeyPrincipalMapper)
                        .build())
                .services(
                    ServiceInfo.fromServiceInstance(new SecuredServiceByUserProfileImpl())
                        .principalMapper(PrincipalMapperAuthExample::userProfilePrincipalMapper)
                        .build()));

    Microservices userProfileCaller =
        Microservices.start(
            new Context()
                .discovery(endpoint -> discovery(service, endpoint))
                .transport(
                    () ->
                        new RSocketServiceTransport().credentialsSupplier(credentialsSupplier())));

    String responseByUserProfile =
        userProfileCaller
            .call()
            .api(SecuredServiceByUserProfile.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'userProfileCaller' response: " + responseByUserProfile);

    Microservices apiKeyCaller =
        Microservices.start(
            new Context()
                .discovery(endpoint -> discovery(service, endpoint))
                .transport(
                    () ->
                        new RSocketServiceTransport().credentialsSupplier(credentialsSupplier())));

    String responseByApiKey =
        apiKeyCaller
            .call()
            .api(SecuredServiceByApiKey.class)
            .hello(UUID.randomUUID().toString())
            .block(Duration.ofSeconds(3));

    System.err.println("### Received 'apiKeyCaller' response: " + responseByApiKey);
  }

  private static Authenticator authenticator() {
    return credentials -> {
      final var headers = credentials(credentials);

      String credsType = headers.get("type"); // credentials type

      if (SecuredServiceByUserProfile.class.getName().equals(credsType)) {
        return Mono.just(authenticateUserProfile(headers));
      }
      if (SecuredServiceByApiKey.class.getName().equals(credsType)) {
        return Mono.just(authenticateApiKey(headers));
      }

      throw new IllegalArgumentException("Wrong namespace: " + credsType);
    };
  }

  private static CredentialsSupplier credentialsSupplier() {
    return (ServiceReference serviceReference, String serviceRole) -> {
      String namespace = serviceReference.namespace(); // decide by service

      if ("securedServiceByUserProfile".equals(namespace)) {
        return userProfileCredentials();
      }
      if ("securedServiceByApiKey".equals(namespace)) {
        return apiKeyCredentials();
      }

      throw new IllegalArgumentException("Wrong namespace: " + namespace);
    };
  }

  private static Map<String, String> authenticateUserProfile(Map<String, String> headers) {
    String username = headers.get("username");
    String password = headers.get("password");

    if ("Alice".equals(username) && "qwerty".equals(password)) {
      HashMap<String, String> authData = new HashMap<>();
      authData.put("name", "Alice");
      authData.put("role", "ADMIN");
      return authData;
    }

    throw new UnauthorizedException("Authentication failed (username or password incorrect)");
  }

  private static Map<String, String> authenticateApiKey(Map<String, String> headers) {
    String apiKey = headers.get("apiKey");

    if ("jasds8fjasdfjasd89fa4k9rjn7ahdfasduf".equals(apiKey)) {
      HashMap<String, String> authData = new HashMap<>();
      authData.put("id", "jasds8fjasdfjasd89fa4k9rjn7ahdfasduf");
      authData.put("permissions", "OPERATIONS:EVENTS:ACTIONS");
      return authData;
    }

    throw new UnauthorizedException("Authentication failed (apiKey incorrect)");
  }

  private static Mono<byte[]> userProfileCredentials() {
    HashMap<String, String> creds = new HashMap<>();
    creds.put("type", SecuredServiceByUserProfile.class.getName());
    creds.put("username", "Alice");
    creds.put("password", "qwerty");
    return Mono.just(encodeCredentials(creds));
  }

  private static Mono<byte[]> apiKeyCredentials() {
    HashMap<String, String> creds = new HashMap<>();
    creds.put("type", SecuredServiceByApiKey.class.getName());
    creds.put("apiKey", "jasds8fjasdfjasd89fa4k9rjn7ahdfasduf");
    return Mono.just(encodeCredentials(creds));
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
