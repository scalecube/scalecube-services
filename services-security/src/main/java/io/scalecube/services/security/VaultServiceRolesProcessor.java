package io.scalecube.services.security;

import io.scalecube.security.vault.VaultServiceRolesInstaller;
import io.scalecube.security.vault.VaultServiceRolesInstaller.ServiceRoles;
import io.scalecube.security.vault.VaultServiceRolesInstaller.ServiceRoles.Role;
import io.scalecube.services.auth.ServiceRolesProcessor;
import io.scalecube.services.methods.ServiceRoleDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class VaultServiceRolesProcessor implements ServiceRolesProcessor {

  private final String environment;
  private final String service;
  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;

  /**
   * Constructor.
   *
   * @param environment logical environment name
   * @param service logical service name
   * @param vaultAddress vaultAddress
   * @param vaultTokenSupplier vaultTokenSupplier
   */
  public VaultServiceRolesProcessor(
      String environment,
      String service,
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier) {
    this.environment = Objects.requireNonNull(environment, "environment");
    this.service = Objects.requireNonNull(service, "service");
    this.vaultAddress = Objects.requireNonNull(vaultAddress, "vaultAddress");
    this.vaultTokenSupplier = Objects.requireNonNull(vaultTokenSupplier, "vaultTokenSupplier");
  }

  @Override
  public void process(Collection<ServiceRoleDefinition> values) {
    VaultServiceRolesInstaller.builder()
        .vaultAddress(vaultAddress)
        .vaultTokenSupplier(vaultTokenSupplier)
        .serviceRolesSources(List.of(() -> toServiceRoles(values)))
        .keyNameSupplier(() -> String.join(".", environment, "identity-key"))
        .roleNameBuilder(role -> String.join(".", environment, service, role))
        .build()
        .install();
  }

  private static ServiceRoles toServiceRoles(Collection<ServiceRoleDefinition> values) {
    return new ServiceRoles()
        .roles(
            values.stream()
                .map(
                    roleDefinition -> {
                      final var role = new Role();
                      role.role(roleDefinition.role());
                      role.permissions(new ArrayList<>(roleDefinition.permissions()));
                      return role;
                    })
                .toList());
  }
}
