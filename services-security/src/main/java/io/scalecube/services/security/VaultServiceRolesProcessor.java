package io.scalecube.services.security;

import io.scalecube.security.vault.VaultServiceRolesInstaller;
import io.scalecube.security.vault.VaultServiceRolesInstaller.ServiceRoles;
import io.scalecube.security.vault.VaultServiceRolesInstaller.ServiceRoles.Role;
import io.scalecube.services.auth.ServiceRolesProcessor;
import io.scalecube.services.methods.ServiceRoleDefinition;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class VaultServiceRolesProcessor implements ServiceRolesProcessor {

  private final String environment;
  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;

  public VaultServiceRolesProcessor(
      String environment,
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier) {
    this.environment = environment;
    this.vaultAddress = vaultAddress;
    this.vaultTokenSupplier = vaultTokenSupplier;
  }

  @Override
  public void process(Collection<ServiceRoleDefinition> values) {
    VaultServiceRolesInstaller.builder()
        .vaultAddress(vaultAddress)
        .vaultTokenSupplier(vaultTokenSupplier)
        .serviceRolesSources(List.of(() -> toServiceRoles(values)))
        .keyNameSupplier(() -> environment + "." + "key")
        .roleNameBuilder(role -> environment + "." + role)
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
