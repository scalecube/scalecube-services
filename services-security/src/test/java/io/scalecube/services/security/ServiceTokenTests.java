package io.scalecube.services.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.security.tokens.jwt.JsonwebtokenResolver;
import io.scalecube.security.tokens.jwt.JwksKeyLocator;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.ServiceRegistration;
import io.scalecube.services.methods.ServiceMethodDefinition;
import io.scalecube.services.methods.ServiceRoleDefinition;
import io.scalecube.services.security.environment.IntegrationEnvironmentFixture;
import io.scalecube.services.security.environment.VaultEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(IntegrationEnvironmentFixture.class)
public class ServiceTokenTests {

  private static final List<ServiceRoleDefinition> SERVICE_ROLES =
      List.of(
          new ServiceRoleDefinition("admin", Set.of("*")),
          new ServiceRoleDefinition("user", Set.of("read", "write")),
          new ServiceRoleDefinition("foo", Set.of("read", "write", "delete")));

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("shouldAuthenticateSuccessfullyMethodSource")
  void shouldAuthenticateSuccessfully(SuccessArgs args, VaultEnvironment vaultEnvironment) {
    final String vaultAddr = vaultEnvironment.vaultAddr();
    final Supplier<CompletableFuture<String>> vaultTokenSupplier =
        () -> CompletableFuture.completedFuture(vaultEnvironment.login());

    final var credentialsSupplier =
        new ServiceTokenCredentialsSupplier(args.environment, vaultAddr, vaultTokenSupplier);

    final var authenticator =
        new ServiceTokenAuthenticator(
            new JsonwebtokenResolver(
                JwksKeyLocator.builder().jwksUri(vaultEnvironment.jwksUri()).build()));

    // Install service roles

    new VaultServiceRolesProcessor(args.environment, vaultAddr, vaultTokenSupplier)
        .process(SERVICE_ROLES);

    // Get service token

    final var tags = new HashMap<String, String>();
    final var credentials =
        credentialsSupplier.credentials(newServiceReference(tags), args.serviceRole).block();

    // Authenticate

    final var principal = authenticator.authenticate(credentials).block();

    assertNotNull(principal, "principal");
    assertEquals(args.serviceRole, principal.role(), "principal.role");
    assertEquals(args.expectedPermissions, principal.permissions(), "principal.permissions");
  }

  private record SuccessArgs(
      String environment, String serviceRole, Set<String> expectedPermissions) {}

  private static Stream<?> shouldAuthenticateSuccessfullyMethodSource() {
    return Stream.of(
        new SuccessArgs("develop", "admin", Set.of("*")),
        new SuccessArgs("develop", "user", Set.of("read", "write")),
        new SuccessArgs("develop", "foo", Set.of("read", "write", "delete")),
        new SuccessArgs("master", "admin", Set.of("*")),
        new SuccessArgs("master", "user", Set.of("read", "write")),
        new SuccessArgs("master", "foo", Set.of("read", "write", "delete")));
  }

  private static ServiceReference newServiceReference(Map<String, String> tags) {
    return new ServiceReference(
        ServiceMethodDefinition.builder().tags(tags).build(),
        new ServiceRegistration(UUID.randomUUID().toString(), tags, Collections.emptyList()),
        ServiceEndpoint.builder().id(UUID.randomUUID().toString()).build());
  }
}
