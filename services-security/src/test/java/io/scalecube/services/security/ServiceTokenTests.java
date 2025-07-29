package io.scalecube.services.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.security.tokens.jwt.JsonwebtokenResolver;
import io.scalecube.security.tokens.jwt.JwksKeyLocator;
import io.scalecube.services.methods.ServiceRoleDefinition;
import io.scalecube.services.security.environment.IntegrationEnvironmentFixture;
import io.scalecube.services.security.environment.VaultEnvironment;
import java.util.List;
import java.util.Set;
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

    new VaultServiceRolesProcessor(args.environment, args.service, vaultAddr, vaultTokenSupplier)
        .process(SERVICE_ROLES);

    // Get service token

    final var credentials = credentialsSupplier.credentials(args.service, args.serviceRole).block();

    // Authenticate

    final var principal = authenticator.authenticate(credentials).block();

    assertNotNull(principal, "principal");
    assertEquals(args.serviceRole, principal.role(), "principal.role");
    assertEquals(args.expectedPermissions, principal.permissions(), "principal.permissions");
  }

  private record SuccessArgs(
      String environment, String service, String serviceRole, Set<String> expectedPermissions) {}

  private static Stream<?> shouldAuthenticateSuccessfullyMethodSource() {
    return Stream.of(
        new SuccessArgs("develop", "app", "admin", Set.of("*")),
        new SuccessArgs("develop", "app", "user", Set.of("read", "write")),
        new SuccessArgs("develop", "app", "foo", Set.of("read", "write", "delete")),
        new SuccessArgs("master", "app", "admin", Set.of("*")),
        new SuccessArgs("master", "app", "user", Set.of("read", "write")),
        new SuccessArgs("master", "app", "foo", Set.of("read", "write", "delete")));
  }
}
