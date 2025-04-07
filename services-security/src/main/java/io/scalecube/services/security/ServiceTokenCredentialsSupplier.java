package io.scalecube.services.security;

import io.scalecube.security.vault.VaultServiceTokenSupplier;
import io.scalecube.services.auth.CredentialsSupplier;
import io.scalecube.services.exceptions.ForbiddenException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import reactor.core.publisher.Mono;

public class ServiceTokenCredentialsSupplier implements CredentialsSupplier {

  private final String environment;
  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;
  private final Collection<String> allowedRoles;

  /**
   * Constructor.
   *
   * @param environment logical environment name
   * @param vaultAddress vaultAddress
   * @param vaultTokenSupplier vaultTokenSupplier
   * @param allowedRoles allowedRoles (optional)
   */
  public ServiceTokenCredentialsSupplier(
      String environment,
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier,
      Collection<String> allowedRoles) {
    this.environment = Objects.requireNonNull(environment, "environment");
    this.vaultAddress = Objects.requireNonNull(vaultAddress, "vaultAddress");
    this.vaultTokenSupplier = Objects.requireNonNull(vaultTokenSupplier, "vaultTokenSupplier");
    this.allowedRoles = allowedRoles;
  }

  @Override
  public Mono<byte[]> credentials(String serviceName, List<String> serviceRoles) {
    return Mono.defer(
        () -> {
          if (serviceRoles == null || serviceRoles.isEmpty()) {
            return Mono.just(new byte[0]);
          }

          String serviceRole = null;

          if (allowedRoles == null || allowedRoles.isEmpty()) {
            serviceRole = serviceRoles.get(0);
          } else {
            for (var allowedRole : allowedRoles) {
              if (serviceRoles.contains(allowedRole)) {
                serviceRole = allowedRole;
              }
            }
          }

          if (serviceRole == null) {
            throw new ForbiddenException("Insufficient permissions");
          }

          return Mono.fromFuture(
                  VaultServiceTokenSupplier.builder()
                      .vaultAddress(vaultAddress)
                      .serviceRole(serviceRole)
                      .vaultTokenSupplier(vaultTokenSupplier)
                      .serviceTokenNameBuilder(
                          (role, tags) -> String.join(".", environment, serviceName, role))
                      .build()
                      .getToken(Collections.emptyMap()))
              .map(String::getBytes);
        });
  }
}
