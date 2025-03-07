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
import java.util.function.Function;
import java.util.function.Supplier;

public class VaultServiceRolesProcessor implements ServiceRolesProcessor {

  private final String vaultAddress;
  private final Supplier<CompletableFuture<String>> vaultTokenSupplier;
  private final Supplier<String> keyNameSupplier;
  private final Function<String, String> roleNameBuilder;

  /**
   * Constructor.
   *
   * @param vaultAddress vaultAddress
   * @param vaultTokenSupplier vaultTokenSupplier
   * @param keyNameSupplier keyNameSupplier
   * @param roleNameBuilder roleNameBuilder
   */
  public VaultServiceRolesProcessor(
      String vaultAddress,
      Supplier<CompletableFuture<String>> vaultTokenSupplier,
      Supplier<String> keyNameSupplier,
      Function<String, String> roleNameBuilder) {
    this.vaultAddress = vaultAddress;
    this.vaultTokenSupplier = vaultTokenSupplier;
    this.keyNameSupplier = keyNameSupplier;
    this.roleNameBuilder = roleNameBuilder;
  }

  @Override
  public void process(Collection<ServiceRoleDefinition> values) {
    new VaultServiceRolesInstaller.Builder()
        .vaultAddress(vaultAddress)
        .vaultTokenSupplier(vaultTokenSupplier)
        .serviceRolesSources(List.of(() -> toServiceRoles(values)))
        .keyNameSupplier(keyNameSupplier)
        .roleNameBuilder(roleNameBuilder)
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
